# orchestrator/o_ccs.py
import logging
logging.basicConfig(level=logging.DEBUG)

import ssl
import asyncio
import websockets
import json
import os
import signal
from datetime import datetime, timezone
from typing import Optional
from dotenv import load_dotenv
import socket
import uuid
import random  

from orchestrator.utils.buffer_manager import buffer_manager
from orchestrator.utils.dispatch import dispatch_incoming  

# lightweight internal HTTP for health + push_config
from fastapi import FastAPI
import uvicorn

logger = logging.getLogger("o_ccs")

# Load environment variables
load_dotenv()

try:
    from orchestrator.o_config import orchestrator_config
    from orchestrator.utils.buffer_manager import OrchestratorBufferManager
except ImportError as e:
    print(f"[O-CCS] Import error: {e}")
    print("[O-CCS] Make sure you're running from the project root directory")
    try:
        from o_config import orchestrator_config
        print("[O-CCS] Using direct import for o_config")
        OrchestratorBufferManager = None
    except ImportError:
        print("[O-CCS] Could not import config manager, exiting")
        exit(1)

CONTROLLER_HOST = os.getenv("CONTROLLER_HOST", "localhost")
CONTROLLER_PORT = int(os.getenv("CONTROLLER_PORT", "8765"))
WS_PATH = os.getenv("CONTROLLER_WS_PATH", "")  

ORCHESTRATOR_ID = os.getenv("ORCHESTRATOR_ID", "orch-001")
CERT_PATH = os.getenv("CERT_PATH", "orchestrator/certs/")
CLIENT_CERT = os.path.join(CERT_PATH, "client_cert.pem")
CLIENT_KEY = os.path.join(CERT_PATH, "client_key.pem")
CA_CERT = os.path.join(CERT_PATH, "ca_cert.pem")
KEEPALIVE_INTERVAL = int(os.getenv("WEBSOCKET_KEEPALIVE_INTERVAL", "10"))

# internal HTTP port (for health + push_config)
ORCH_HTTP_PORT = int(os.getenv("ORCHESTRATOR_HTTP_PORT", "8011"))

config_manager = None
connected = False
shutdown_event = asyncio.Event()

# --- simple metrics ---
_keepalives_sent = 0
_frames_rx = 0
_frames_tx = 0

# current websocket (used by /push_config)
_ws: Optional[websockets.WebSocketClientProtocol] = None


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def build_ssl_context():
    """Prefer verified TLS if CA is present; otherwise fall back (dev only)."""
    if os.getenv("SSL_DISABLED", "false").lower() == "true":
        logger.warning("[O-CCS] SSL disabled via environment variable")
        return None

    ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)

    if os.path.exists(CA_CERT):
        ctx.load_verify_locations(CA_CERT)
        ctx.verify_mode = ssl.CERT_REQUIRED
        ctx.check_hostname = True
        logger.info("[O-CCS] Using CA verification")
    else:
        # Dev-friendly fallback
        ctx.verify_mode = ssl.CERT_NONE
        ctx.check_hostname = False
        logger.warning("[O-CCS] CA cert missing; disabling verification (dev)")

    if os.path.exists(CLIENT_CERT) and os.path.exists(CLIENT_KEY):
        try:
            ctx.load_cert_chain(CLIENT_CERT, CLIENT_KEY)
            logger.info(f"[O-CCS] Loaded client certificates: {CLIENT_CERT}")
        except Exception as e:
            logger.error(f"[O-CCS] Failed to load client certificates: {e}")

    return ctx


def _get_local_ip():
    try:
        hostname = socket.gethostname()
        return socket.gethostbyname(hostname)
    except Exception:
        return "0.0.0.0"


def build_handshake_from_config(cfg: dict, ssl_context) -> dict:
    """Always returns a valid handshake dict with 'data.metadata' object."""
    organization = (cfg or {}).get("organization", {}) or {}
    features = ((cfg or {}).get("features") or organization.get("features") or {})

    orch_id = (cfg or {}).get("orchestrator_id") or organization.get("orchestrator_id") or ORCHESTRATOR_ID
    name = organization.get("name") or f"Organization {orch_id}"
    location = organization.get("location") or "unknown"

    metadata = {
        "version": (cfg or {}).get("metadata", {}).get("version", "1.0.0"),
        "hostname": socket.gethostname(),
        "ip": _get_local_ip(),
        "ssl_enabled": bool(ssl_context),
        "name": name,
        "location": location,
        "features": features,
    }

    handshake = {
        "type": "handshake",
        "service": "orchestrator",
        "data": {
            "orchestrator_id": orch_id,
            "metadata": metadata,
        },
        "timestamp": datetime.utcnow().isoformat(),
    }
    return handshake


def _validate_handshake(h: dict) -> tuple[bool, str]:
    """Client-side schema assert before send (belt & suspenders)."""
    if h.get("type") != "handshake":
        return False, "type must be 'handshake'"
    if h.get("service") != "orchestrator":
        return False, "service must be 'orchestrator'"
    d = h.get("data")
    if not isinstance(d, dict):
        return False, "'data' missing or not a dict"
    if "orchestrator_id" not in d or not d["orchestrator_id"]:
        return False, "missing orchestrator_id"
    m = d.get("metadata")
    if not isinstance(m, dict):
        return False, "'metadata' missing or not a dict"
    return True, "ok"


async def send_keepalive(ws):
    global _keepalives_sent, _frames_tx
    try:
        while not shutdown_event.is_set():
            msg = {
                "type": "keepalive",
                "orchestrator_id": ORCHESTRATOR_ID,
                "timestamp": datetime.utcnow().isoformat(),
            }
            await ws.send(json.dumps(msg))
            _keepalives_sent += 1
            _frames_tx += 1
            # UI remains responsive even if ack drops
            try:
                if buffer_manager:
                    buffer_manager.update_active_user(
                        user_id=ORCHESTRATOR_ID,
                        orch_id=ORCHESTRATOR_ID,
                        metadata={"last_seen": now_iso()},
                    )
            except Exception as e:
                logger.warning(f"[O-CCS] active_user refresh failed: {e}")
            await asyncio.sleep(KEEPALIVE_INTERVAL)
    except asyncio.CancelledError:
        pass
    except Exception as e:
        logger.error(f"[O-CCS] keepalive error: {e}")


async def receive_loop(ws):
    """Ultra-thin: parse JSON and hand off to utils.dispatch."""
    global _frames_rx
    try:
        async for raw in ws:
            try:
                data = json.loads(raw)
            except json.JSONDecodeError:
                logger.warning("[O-CCS] Received non-JSON message")
                continue
            _frames_rx += 1
            dispatch_incoming(data, ORCHESTRATOR_ID, orchestrator_config)
    except asyncio.CancelledError:
        pass
    except Exception as e:
        logger.error(f"[O-CCS] receive_loop error: {e}")


async def connect():
    global connected, config_manager, _ws
    shutdown_event.clear()

    try:
        config_manager = orchestrator_config
        # optional: init local manager instance if needed
        if OrchestratorBufferManager:
            _ = OrchestratorBufferManager()
    except Exception as e:
        logger.warning(f"[O-CCS] Warning: Could not initialize managers: {e}")

    ssl_context = build_ssl_context()
    protocol = "wss" if ssl_context else "ws"
    uri = f"{protocol}://{CONTROLLER_HOST}:{CONTROLLER_PORT}{WS_PATH}"

    for attempt in range(10):
        if shutdown_event.is_set():
            break

        try:
            logger.info(f"[O-CCS] Connecting to {uri} (attempt {attempt + 1})")
            try:
                websocket = await asyncio.wait_for(
                    websockets.connect(
                        uri,
                        ssl=ssl_context,
                        ping_interval=KEEPALIVE_INTERVAL * 1.5,
                        ping_timeout=KEEPALIVE_INTERVAL,
                        max_queue=64,
                    ),
                    timeout=10
                )
                logger.info("[O-CCS] WebSocket connection established")
            except asyncio.TimeoutError:
                logger.warning("[O-CCS] Connection timed out")
                await asyncio.sleep(3 + random.random() * 5)
                continue
            except Exception as e:
                logger.error(f"[O-CCS] Connection failed: {e}")
                await asyncio.sleep(3 + random.random() * 5)
                continue

            cfg = {}
            if config_manager:
                try:
                    cfg = config_manager.get_config()
                except Exception as e:
                    logger.warning(f"[O-CCS] Could not read O-Config, using fallbacks: {e}")

            handshake_msg = build_handshake_from_config(cfg, ssl_context)
            ok, why = _validate_handshake(handshake_msg)
            if not ok:
                logger.error(f"[O-CCS] Invalid handshake: {why}")
                await websocket.close()
                await asyncio.sleep(2)
                continue

            logger.debug("[O-CCS] Final handshake_msg being sent:\n%s",
                         json.dumps(handshake_msg, indent=2))
            await websocket.send(json.dumps(handshake_msg))

            try:
                response_raw = await asyncio.wait_for(websocket.recv(), timeout=10)
                try:
                    response = json.loads(response_raw)
                except json.JSONDecodeError:
                    logger.error("[O-CCS] Handshake response was not JSON.")
                    await websocket.close()
                    continue

                logger.info(f"[O-CCS] Received handshake response: {response}")
                if response.get("type") != "handshake_ack":
                    raise Exception("Invalid handshake_ack response")
            except asyncio.TimeoutError:
                logger.warning("[O-CCS] Handshake response timeout")
                await websocket.close()
                continue

            connected = True
            _ws = websocket

            send_task = asyncio.create_task(send_keepalive(websocket))
            recv_task = asyncio.create_task(receive_loop(websocket))
            shutdown_task = asyncio.create_task(shutdown_event.wait())

            done, pending = await asyncio.wait(
                [send_task, recv_task, shutdown_task],
                return_when=asyncio.FIRST_COMPLETED
            )

            for task in pending:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

            logger.info("[O-CCS] Client connection shutting down.")
            connected = False
            _ws = None
            return

        except Exception as e:
            logger.error(f"[O-CCS] Connection attempt {attempt + 1} failed: {e}")

        await asyncio.sleep(3 + random.random() * 5)


def _install_signal_handlers(loop):
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, shutdown_event.set)
        except NotImplementedError:
            # Windows / non-main thread
            pass


# -------------------------
# Internal HTTP (healthz, push_config)
# -------------------------
http_app = FastAPI(title="O-CCS Internal")

@http_app.get("/healthz")
def healthz():
    return {
        "ok": True,
        "orchestrator_id": ORCHESTRATOR_ID,
        "connected": connected,
        "keepalives_sent": _keepalives_sent,
        "frames_rx": _frames_rx,
        "frames_tx": _frames_tx,
        "ts": now_iso(),
    }

@http_app.post("/push_config")
async def push_config():
    if _ws is None or not connected:
        return {"status": "not_connected"}

    #  force-refresh from disk so this process sees O-GUI’s latest write
    try:
        orchestrator_config.reload_config()
    except Exception as e:
        logger.warning(f"[O-CCS] reload_config failed before push: {e}")

    cfg = orchestrator_config.get_config()
    frame = {
        "type": "o_config_snapshot",
        "orchestrator_id": ORCHESTRATOR_ID,
        "timestamp": now_iso(),
        "data": cfg,
    }
    await _ws.send(json.dumps(frame))
    global _frames_tx
    _frames_tx += 1
    return {"status": "sent", "bytes": len(json.dumps(cfg))}


async def _start_http_server():
    config = uvicorn.Config(http_app, host="0.0.0.0", port=ORCH_HTTP_PORT, log_level="info", loop="asyncio")
    server = uvicorn.Server(config)
    await server.serve()


if __name__ == "__main__":
    # run both: internal HTTP + websocket connector
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    _install_signal_handlers(loop)
    # start HTTP server
    loop.create_task(_start_http_server())
    logger.info(f"[O-CCS] Internal HTTP at http://localhost:{ORCH_HTTP_PORT}")
    # start WS client
    loop.run_until_complete(connect())

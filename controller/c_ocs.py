import logging
logging.basicConfig(level=logging.DEBUG)

import ssl
import asyncio
import websockets
import json
import os
import copy
from datetime import datetime, timezone
from typing import Dict, Optional
from dotenv import load_dotenv

from fastapi import FastAPI
import uvicorn

from controller.c_config import controller_config
from controller.utils.controller_state import remove_orchestrator, list_orchestrators
from controller.utils.dispatch import handle_handshake, dispatch_incoming
from controller.utils.buffer_manager import controller_buffers

load_dotenv()
logger = logging.getLogger("c_ocs")

HOST = os.getenv("CONTROLLER_HOST", "localhost")
PORT = int(os.getenv("CONTROLLER_PORT", "8765"))

CERT_PATH = os.getenv("CERT_PATH", "controller/certs/")
CERT_FILE = os.path.join(CERT_PATH, "server_cert.pem")
KEY_FILE = os.path.join(CERT_PATH, "server_key.pem")
CA_FILE = os.path.join(CERT_PATH, "ca_cert.pem")

KEEPALIVE_INTERVAL = int(os.getenv("WEBSOCKET_KEEPALIVE_INTERVAL", "10"))

# HTTP port for internal C-OCS endpoints
OCS_HTTP_PORT = int(os.getenv("CONTROLLER_OCS_HTTP_PORT", "8010"))

# Cache last-sent features so we only push deltas (shared with dispatcher)
_last_sent_features: Dict[str, dict] = {}

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def build_ssl_context():
    if os.getenv("SSL_DISABLED", "false").lower() == "true":
        logger.warning("[C-OCS] SSL disabled via env var")
        return None
    if not (os.path.exists(CERT_FILE) and os.path.exists(KEY_FILE)):
        logger.warning("[C-OCS] SSL certs missing; starting without TLS")
        return None
    try:
        ctx = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
        ctx.load_cert_chain(certfile=CERT_FILE, keyfile=KEY_FILE)
        if os.path.exists(CA_FILE):
            ctx.load_verify_locations(CA_FILE)
            ctx.verify_mode = ssl.CERT_OPTIONAL  # set CERT_REQUIRED for strict mTLS
            logger.info("[C-OCS] CA loaded; client certs optional")
        else:
            ctx.verify_mode = ssl.CERT_NONE
        return ctx
    except Exception as e:
        logger.error(f"[C-OCS] Failed to create SSL context: {e}")
        return None

# ---------------------------
# Dispatcher-driven handler
# ---------------------------
async def handler(ws):
    orch_id: Optional[str] = None

    # 1) Receive handshake frame
    try:
        raw = await asyncio.wait_for(ws.recv(), timeout=15)
        handshake = json.loads(raw)
    except asyncio.TimeoutError:
        await ws.close(code=4000, reason="handshake_timeout"); return
    except Exception:
        await ws.close(code=4001, reason="handshake_recv_error"); return

    # 2) Delegate handshake to utils/dispatch
    orch_id = await handle_handshake(ws, handshake, _last_sent_features)
    if not orch_id:
        return  # handshake failed; connection closed by dispatcher

    # 3) Main loop — pure dispatch per frame
    try:
        async for raw in ws:
            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                await ws.send(json.dumps({"type": "error", "reason": "invalid_json_in_stream"}))
                continue
            await dispatch_incoming(ws, orch_id, msg, _last_sent_features)
    except asyncio.CancelledError:
        pass
    finally:
        if orch_id:
            remove_orchestrator(orch_id)
            _last_sent_features.pop(orch_id, None)
            try:
                controller_config.unregister_orchestrator(orch_id)
            except Exception as e:
                logger.warning(f"[C-OCS] unregister_orchestrator error: {e}")
            logger.info(f"[C-OCS] Disconnected: {orch_id}")
            controller_buffers.add_activity("disconnect", {"orch_id": orch_id})

# ---------------------------
# Provisioning delta watcher
# ---------------------------
async def provisioning_watcher(interval_seconds: int = 2):
    from controller.utils.provisioning import send_provisioning_to  # local import to avoid cycles
    while True:
        try:
            controller_config.reload_config()  # pick up C-GUI edits from disk
            cfg = controller_config.get_config()
            orgs = cfg.get("organizations", {})

            # Iterate over currently connected orchestrators
            for org_id, info in list(list_orchestrators(public=False).items()):
                ws = info.get("ws")
                if ws is None:
                    continue
                org_cfg = orgs.get(org_id, {})
                current_features = org_cfg.get("features", {})
                last_features = _last_sent_features.get(org_id)
                if last_features is None or current_features != last_features:
                    logger.info(f"[C-OCS] Feature delta for {org_id} -> pushing provisioning")
                    try:
                        await send_provisioning_to(ws, org_id, controller_config)
                        _last_sent_features[org_id] = copy.deepcopy(current_features)
                        controller_buffers.add_activity("provisioning_sent", {"orch_id": org_id, "features": _last_sent_features[org_id]})
                    except Exception as e:
                        logger.warning(f"[C-OCS] watcher provisioning failed for {org_id}: {e}")
                        controller_buffers.add_activity("provisioning_error", {"orch_id": org_id, "error": str(e)})
        except Exception as e:
            logger.warning(f"[C-OCS] watcher error: {e}")
        await asyncio.sleep(interval_seconds)

# ---------------------------
# HTTP: internal metrics API
# ---------------------------
http_app = FastAPI(title="C-OCS Internal")

@http_app.get("/ocx/activities")
def ocx_activities(limit: int = 50, offset: int = 0):
    return controller_buffers.get_recent_activities(limit=limit, offset=offset)

@http_app.get("/ocx/buffer_stats")
def ocx_buffer_stats():
    return controller_buffers.get_stats()

@http_app.get("/ocx/orchestrators")
def ocx_orchestrators():
    """
    Returns live orchestrators as seen by this C-OCS process.
    We overlay current controller features (from controller_config)
    onto the handshake metadata so the view reflects latest toggles.
    """
    live = list_orchestrators(public=True)   # handshake-time metadata
    cfg_orgs = controller_config.get_config().get("organizations", {})
    out = {}
    for org_id, info in live.items():
        md = dict(info.get("metadata", {}))  # copy
        current = cfg_orgs.get(org_id, {}).get("features", {})
        if current:
            md["features"] = current
        out[org_id] = {
            "last_seen": info.get("last_seen"),
            "metadata": md,
        }
    return out

# Expose last received O-Config snapshots (optional, if stored by dispatch)
@http_app.get("/ocx/o_config/{org_id}")
def ocx_o_config(org_id: str):
    # read the freshest on-disk config, because dispatch wrote snapshots there
    controller_config.reload_config()
    cfg = controller_config.get_config()
    org = (cfg.get("organizations") or {}).get(org_id)
    if not org:
        return {"error": "unknown org_id"}, 404
    snap = org.get("o_config_snapshot")
    if not snap:
        return {"error": "no snapshot for this org"}, 404
    return snap

async def start_http_server():
    config = uvicorn.Config(http_app, host="0.0.0.0", port=OCS_HTTP_PORT, log_level="info", loop="asyncio")
    server = uvicorn.Server(config)
    await server.serve()

# ---------------------------
# Entrypoint
# ---------------------------
async def main():
    # Start HTTP server for internal metrics
    asyncio.create_task(start_http_server())
    logger.info(f"[C-OCS] HTTP metrics on http://localhost:{OCS_HTTP_PORT}")

    ctx = build_ssl_context()
    protocol = "wss" if ctx else "ws"
    logger.info(f"[C-OCS] Starting WebSocket server on {protocol}://{HOST}:{PORT}")
    async with websockets.serve(handler, HOST, PORT, ssl=ctx):
        asyncio.create_task(provisioning_watcher(2))
        await asyncio.Future()  # run forever

if __name__ == "__main__":
    asyncio.run(main())

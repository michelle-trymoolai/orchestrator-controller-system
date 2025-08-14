# controller/utils/dispatch.py
import copy
import logging
import json
from typing import Any, Dict
from datetime import datetime, timezone

from controller.utils.controller_state import mark_handshake, mark_keepalive
from controller.utils.buffer_manager import controller_buffers
from controller.c_config import controller_config
from controller.utils.provisioning import send_provisioning_to

logger = logging.getLogger("c_dispatch")

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def _validate_handshake_server_side(h: dict) -> tuple[bool, str]:
    if h.get("type") != "handshake":
        return False, "type must be 'handshake'"
    d = h.get("data")
    if not isinstance(d, dict):
        return False, "'data' missing or not a dict"
    orch_id = d.get("orchestrator_id")
    if not orch_id:
        return False, "missing orchestrator_id"
    md = d.get("metadata")
    if not isinstance(md, dict):
        return False, "'metadata' missing or not a dict"
    return True, "ok"

def _persist_presence(orch_id: str, metadata: dict | None = None):
    """Write last_seen + status into controller_config so C-GUI can read presence."""
    try:
        cfg = controller_config.get_config()
        orgs = cfg.setdefault("organizations", {})
        org = orgs.setdefault(orch_id, {})
        org["last_seen"] = _now_iso()
        org["status"] = "active"
        if metadata:
            org["metadata"] = metadata
        controller_config.update_config(cfg)
    except Exception as e:
        logger.warning(f"[C-DISPATCH] persist presence failed for {orch_id}: {e}")

async def handle_handshake(ws, handshake: Dict[str, Any], last_sent_features: Dict[str, dict]):
    ok, why = _validate_handshake_server_side(handshake)
    if not ok:
        await ws.send(json.dumps({"type": "error", "reason": f"invalid_handshake: {why}"}))
        await ws.close(code=4003, reason=f"invalid_handshake: {why}")
        return None  # signals caller to stop

    data = handshake.get("data") or {}
    orch_id = data.get("orchestrator_id")
    metadata = data.get("metadata") or {}

    # registry + config
    mark_handshake(orch_id, ws, metadata)
    try:
        controller_config.register_orchestrator(
            orch_id,
            {"name": metadata.get("name") or f"Organization {orch_id}",
             "location": metadata.get("location") or "unknown",
             "metadata": metadata},
        )
    except Exception as e:
        logger.warning(f"[C-DISPATCH] register_orchestrator error: {e}")

    # persist presence for C-GUI
    _persist_presence(orch_id, metadata)

    # ack + initial provisioning
    await ws.send(json.dumps({"type": "handshake_ack", "orchestrator_id": orch_id}))
    controller_buffers.add_activity("handshake_ack_sent", {"orch_id": orch_id})

    try:
        await send_provisioning_to(ws, orch_id, controller_config)
        org_cfg = controller_config.get_config().get("organizations", {}).get(orch_id, {})
        last_sent_features[orch_id] = copy.deepcopy(org_cfg.get("features", {}))
        controller_buffers.add_activity("provisioning_sent", {"orch_id": orch_id, "features": last_sent_features[orch_id]})
    except Exception as e:
        controller_buffers.add_activity("provisioning_error", {"orch_id": orch_id, "error": str(e)})
        logger.warning(f"[C-DISPATCH] initial provisioning failed for {orch_id}: {e}")

    return orch_id

async def dispatch_incoming(ws, orch_id: str, msg: Dict[str, Any], last_sent_features: Dict[str, dict]):
    mtype = msg.get("type")

    if mtype == "keepalive":
        mark_keepalive(orch_id)
        await ws.send(json.dumps({"type": "keepalive_ack", "timestamp": _now_iso()}))
        controller_buffers.add_activity("keepalive_ack", {"orch_id": orch_id})
        _persist_presence(orch_id)  # keep file-backed presence fresh

    elif mtype == "provisioning_request":
        try:
            await send_provisioning_to(ws, orch_id, controller_config)
            org_cfg = controller_config.get_config().get("organizations", {}).get(orch_id, {})
            last_sent_features[orch_id] = copy.deepcopy(org_cfg.get("features", {}))
            controller_buffers.add_activity("provisioning_sent", {"orch_id": orch_id, "features": last_sent_features[orch_id]})
        except Exception as e:
            controller_buffers.add_activity("provisioning_error", {"orch_id": orch_id, "error": str(e)})
            logger.warning(f"[C-DISPATCH] provisioning_request failed for {orch_id}: {e}")

    #  accept and store O-Config snapshots from the orchestrator
    elif mtype == "o_config_snapshot":
        try:
            cfg = controller_config.get_config()
            orgs = cfg.setdefault("organizations", {})
            org = orgs.setdefault(orch_id, {})
            org["o_config_snapshot"] = {
                "received_at": _now_iso(),
                "data": msg.get("data", {}) or {},
            }
            controller_config.update_config(cfg)
            controller_buffers.add_activity("o_config_snapshot_rx", {"orch_id": orch_id})
            logger.info(f"[C-DISPATCH] stored o_config_snapshot for {orch_id}")
        except Exception as e:
            controller_buffers.add_activity("o_config_snapshot_error", {"orch_id": orch_id, "error": str(e)})
            logger.warning(f"[C-DISPATCH] o_config_snapshot store failed for {orch_id}: {e}")

    elif mtype == "prompt_response":
        controller_buffers.add_activity("prompt_response_rx", {"orch_id": orch_id, "msg": msg})
        logger.debug(f"[C-DISPATCH] prompt_response from {orch_id}")

    elif mtype == "prompt":
        controller_buffers.add_activity("prompt_rx", {"orch_id": orch_id, "msg": msg})
        logger.debug(f"[C-DISPATCH] prompt from {orch_id}")

    else:
        controller_buffers.add_activity("unknown_msg", {"orch_id": orch_id, "msg": msg})
        logger.debug(f"[C-DISPATCH] unhandled: {mtype}")

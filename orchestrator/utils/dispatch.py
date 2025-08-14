# orchestrator/utils/dispatch.py
import uuid
import logging
from typing import Any, Dict, Optional
from datetime import datetime, timezone

from orchestrator.utils.buffer_manager import buffer_manager
from orchestrator.utils.provisioning import apply_provisioning

logger = logging.getLogger("o_dispatch")


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _bm_update_active_user(orch_id: str, meta: Optional[Dict[str, Any]] = None):
    try:
        if buffer_manager:
            buffer_manager.update_active_user(
                user_id=orch_id, orch_id=orch_id, metadata=meta or {}
            )
    except Exception as e:
        logger.warning(f"[O-DISPATCH] active_user update failed: {e}")


def _bm_add_prompt(
    orch_id: str,
    prompt_id: str,
    prompt_text: str,
    meta: Optional[Dict[str, Any]] = None,
):
    try:
        if buffer_manager:
            buffer_manager.add_prompt(
                prompt_id=prompt_id,
                user_id=orch_id,
                prompt=prompt_text,
                response=None,
                metadata=meta or {},
            )
    except Exception as e:
        logger.warning(f"[O-DISPATCH] add_prompt failed: {e}")


def _bm_update_prompt(
    prompt_id: Optional[str],
    resp_text: str,
    meta: Optional[Dict[str, Any]] = None,
):
    if not prompt_id:
        logger.warning("[O-DISPATCH] update_prompt called with no prompt_id")
        return
    try:
        if buffer_manager:
            buffer_manager.update_prompt(
                prompt_id=prompt_id, response=resp_text, metadata=meta or {}
            )
    except Exception as e:
        logger.warning(f"[O-DISPATCH] update_prompt failed: {e}")


def _persist_last_seen(_orchestrator_config):
    """
    Persist a lightweight presence touch so /config shows fresh last_seen/status.
    """
    try:
        cfg = _orchestrator_config.get_config()
        cfg["last_seen"] = _now_iso()
        cfg["status"] = "active"
        _orchestrator_config.update_config(cfg)
    except Exception as e:
        logger.warning(f"[O-DISPATCH] persist last_seen failed: {e}")


def _persist_provisioning_to_config(msg: Dict[str, Any], _orchestrator_config):
    """
    Merge provisioning payload (esp. features) into orchestrator_config.json.
    Keeps O-GUI /config and subsequent /push_config snapshots in sync.
    """
    try:
        data = msg.get("data", {}) or {}
        cfg = _orchestrator_config.get_config()

        # Merge top-level 'features' if provided
        prov_features = data.get("features")
        if isinstance(prov_features, dict):
            cfg.setdefault("features", {}).update(prov_features)

        # Optional: merge other top-level provisioning knobs here if you add them
        # e.g., firewall/cache params, etc.

        _orchestrator_config.update_config(cfg)
    except Exception as e:
        logger.warning(f"[O-DISPATCH] persist provisioning failed: {e}")


def dispatch_incoming(msg: Dict[str, Any], orchestrator_id: str, _orchestrator_config) -> None:
    """
    Pure dispatcher for O-CCS receive loop.
    No I/O side-effects beyond buffer/provisioning/config calls.
    """
    mtype = msg.get("type")

    if mtype == "handshake_ack":
        _bm_update_active_user(orchestrator_id, meta=msg.get("metadata"))
        _persist_last_seen(_orchestrator_config)
        logger.info(f"[O-DISPATCH:{orchestrator_id}] handshake_ack")

    elif mtype == "keepalive_ack":
        _bm_update_active_user(orchestrator_id, meta=msg.get("metadata"))
        _persist_last_seen(_orchestrator_config)
        logger.debug(f"[O-DISPATCH:{orchestrator_id}] keepalive_ack")

    elif mtype == "prompt":
        prompt_id = msg.get("prompt_id") or str(uuid.uuid4())
        prompt_text = msg.get("prompt", "")
        _bm_add_prompt(
            orchestrator_id,
            prompt_id,
            prompt_text,
            {"source": "controller"},
        )
        logger.info(f"[O-DISPATCH:{orchestrator_id}] prompt stored {prompt_id}")

    elif mtype == "prompt_response":
        _bm_update_prompt(
            msg.get("prompt_id"),
            msg.get("response", ""),
            {"source": "controller"},
        )
        logger.info(f"[O-DISPATCH:{orchestrator_id}] prompt_response applied")

    elif mtype == "provisioning":
        # 1) Apply in-memory toggles (immediate effect for running services)
        apply_provisioning(msg.get("data", {}), _orchestrator_config)
        # 2) Persist to orchestrator_config.json so O-GUI /config & /push_config match
        _persist_provisioning_to_config(msg, _orchestrator_config)
        logger.info(f"[O-DISPATCH:{orchestrator_id}] provisioning applied & persisted")

    elif mtype == "command":
        logger.info(f"[O-DISPATCH] command: {msg}")

    elif mtype == "o_config_snapshot_ack":
        # Optional: track ack from controller when it stores our snapshot
        logger.debug(f"[O-DISPATCH:{orchestrator_id}] o_config_snapshot_ack: {msg}")

    else:
        logger.debug(f"[O-DISPATCH] unhandled: {mtype}")

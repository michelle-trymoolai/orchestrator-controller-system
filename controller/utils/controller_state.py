# controller/utils/controller_state.py
import threading
from datetime import datetime, timezone
from typing import Dict, Any, Optional

_lock = threading.RLock()
_connected: Dict[str, dict] = {}  

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def mark_handshake(orch_id: str, ws, metadata: Optional[Dict[str, Any]] = None) -> None:
    with _lock:
        _connected[orch_id] = {
            "ws": ws,
            "last_seen": _now_iso(),
            "metadata": dict(metadata or {}),
        }

def mark_keepalive(orch_id: str, metadata: Optional[Dict[str, Any]] = None) -> None:
    with _lock:
        if orch_id in _connected:
            _connected[orch_id]["last_seen"] = _now_iso()
            if metadata:
                # merge rather than replace
                _connected[orch_id]["metadata"].update(metadata)

def remove_orchestrator(orch_id: str) -> None:
    with _lock:
        _connected.pop(orch_id, None)

def get_ws(orch_id: str):
    with _lock:
        entry = _connected.get(orch_id)
        return entry["ws"] if entry else None

def list_orchestrators(public: bool = True) -> Dict[str, Any]:
    with _lock:
        if public:
            # return shallow copies to avoid accidental external mutation
            return {
                k: {
                    "last_seen": v.get("last_seen"),
                    "metadata": dict(v.get("metadata", {})),
                }
                for k, v in _connected.items()
            }
        else:
            # internal callers may need ws handle; still return a shallow copy
            return {k: dict(v) for k, v in _connected.items()}

# Optional helper
def get_orchestrator_public(org_id: str) -> Optional[Dict[str, Any]]:
    with _lock:
        v = _connected.get(org_id)
        if not v:
            return None
        return {
            "last_seen": v.get("last_seen"),
            "metadata": dict(v.get("metadata", {})),
        }

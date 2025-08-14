# controller/utils/buffer_manager.py
import threading
from collections import deque
from typing import Dict, Any, List, Optional
from datetime import datetime, timezone
import queue

class ControllerBufferManager:
    """Thread-safe buffer manager for Controller operations: activity log + GUI→OCS update queue."""

    def __init__(self, activity_buffer_size: int = 5000, queue_maxsize: int = 0):
        self._lock = threading.RLock()

        # Activity log (most recent last; we'll reverse on read)
        self._activity = deque(maxlen=activity_buffer_size)

        # Non-blocking queue from C-GUI to C-OCS (0 == infinite)
        self._gui_to_ocs = queue.Queue(maxsize=queue_maxsize)

        self._stats = {
            "total_activities": 0,
            "total_gui_updates": 0,
        }

    # ---------- Activity ----------
    def add_activity(self, activity_type: str, metadata: Optional[Dict[str, Any]] = None) -> None:
        with self._lock:
            self._activity.append({
                "activity_id": f"act-{datetime.now(timezone.utc).timestamp()}",
                "activity_type": activity_type,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "metadata": metadata or {},
            })
            self._stats["total_activities"] += 1

    def get_recent_activities(self, limit: int = 50, offset: int = 0) -> List[Dict[str, Any]]:
        with self._lock:
            items = list(self._activity)  # oldest → newest
            items.reverse()               # newest → oldest
            return items[offset: offset + limit]

    # ---------- GUI → OCS queue ----------
    def queue_gui_update(self, update_type: str, data: Dict[str, Any]) -> None:
        try:
            self._gui_to_ocs.put_nowait({
                "type": update_type,
                "data": data,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            })
            with self._lock:
                self._stats["total_gui_updates"] += 1
        except queue.Full:
            print("[CONTROLLER-BUFFER] GUI→OCS queue full; dropping update")

    def get_gui_update_nowait(self) -> Optional[Dict[str, Any]]:
        try:
            return self._gui_to_ocs.get_nowait()
        except queue.Empty:
            return None

    # ---------- Stats ----------
    def get_stats(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "activity_buffer": {
                    "current_entries": len(self._activity),
                    "max_size": self._activity.maxlen,
                    "total_entries": self._stats["total_activities"],
                },
                "gui_to_ocs_queue": {
                    "pending_updates": self._gui_to_ocs.qsize(),
                    "total_updates": self._stats["total_gui_updates"],
                }
            }

#  Shared singleton used by BOTH C-OCS and C-GUI
controller_buffers = ControllerBufferManager()

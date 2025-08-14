import threading
import time
from collections import deque
from typing import Dict, Any, List, Optional
from datetime import datetime, timezone


class OrchestratorBufferManager:
    """
    Thread-safe buffer manager for the Orchestrator:
    - Prompts (dict keyed by prompt_id) + recency order
    - Tasks   (dict keyed by task_id)   + recency order
    - Active users (heartbeat-style presence)
    """

    def __init__(self, prompt_buffer_size: int = 10000, task_buffer_size: int = 5000):
        self._lock = threading.RLock()

        # --- Prompts ---
        self._prompt_max = prompt_buffer_size
        self._prompt_store: Dict[str, Dict[str, Any]] = {}    # prompt_id -> entry
        self._prompt_order: deque[str] = deque()              # recency order of prompt_ids

        # --- Tasks ---
        self._task_max = task_buffer_size
        self._task_store: Dict[str, Dict[str, Any]] = {}      # task_id -> entry
        self._task_order: deque[str] = deque()                # recency order of task_ids

        # --- Active users ---
        self.active_users: Dict[str, Dict[str, Any]] = {}

        # --- Stats ---
        self.stats = {
            "total_prompts": 0,
            "total_tasks": 0,
            "total_users": 0,
        }

    # =======================
    # Active Users
    # =======================
    def update_active_user(
        self,
        user_id: str,
        orch_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Create or refresh an active user record."""
        with self._lock:
            is_new = user_id not in self.active_users
            self.active_users[user_id] = {
                "orch_id": orch_id,
                "last_seen": time.time(),
                "login_timestamp": self.active_users.get(user_id, {}).get("login_timestamp", time.time()),
                "metadata": metadata or {},
            }
            if is_new:
                self.stats["total_users"] += 1

    def remove_active_user(self, user_id: str) -> None:
        with self._lock:
            self.active_users.pop(user_id, None)

    def cleanup_expired(self, timeout_seconds: int = 1800) -> None:
        """Drop users whose last_seen is older than timeout_seconds."""
        with self._lock:
            now = time.time()
            expired = [
                uid
                for uid, info in self.active_users.items()
                if now - info.get("last_seen", 0) > timeout_seconds
            ]
            for uid in expired:
                self.active_users.pop(uid, None)

    # ✅ NEW: convenience readers for active users
    def get_active_users(self) -> List[Dict[str, Any]]:
        """Return active users sorted by most recent last_seen (desc)."""
        with self._lock:
            items: List[Dict[str, Any]] = []
            for uid, info in self.active_users.items():
                items.append({
                    "user_id": uid,
                    "orch_id": info.get("orch_id"),
                    "last_seen": info.get("last_seen"),
                    "login_timestamp": info.get("login_timestamp"),
                    "metadata": info.get("metadata", {}),
                })
            items.sort(key=lambda x: (x.get("last_seen") or 0), reverse=True)
            return items

    def get_active_user(self, user_id: str) -> Optional[Dict[str, Any]]:
        """Return a single active user record, or None."""
        with self._lock:
            info = self.active_users.get(user_id)
            if not info:
                return None
            return {
                "user_id": user_id,
                "orch_id": info.get("orch_id"),
                "last_seen": info.get("last_seen"),
                "login_timestamp": info.get("login_timestamp"),
                "metadata": info.get("metadata", {}),
            }

    # =======================
    # Prompts
    # =======================
    def add_prompt(
        self,
        prompt_id: str,
        user_id: str,
        prompt: str,
        response: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Create a prompt record (addressable by prompt_id) and track recency."""
        with self._lock:
            entry = {
                "prompt_id": prompt_id,
                "user_id": user_id,
                "prompt": prompt,
                "response": response,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "updated_at": None,
                "metadata": metadata or {},
            }
            self._prompt_store[prompt_id] = entry
            self._push_prompt_id(prompt_id)
            self.stats["total_prompts"] += 1

    def get_prompt(self, prompt_id: str) -> Optional[Dict[str, Any]]:
        """Fetch a single prompt record by id."""
        with self._lock:
            return self._prompt_store.get(prompt_id)

    def update_prompt(
        self,
        prompt_id: str,
        response: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Update response and metadata for a prompt by id."""
        with self._lock:
            entry = self._prompt_store.get(prompt_id)
            if not entry:
                return  # silently ignore for robustness
            if response is not None:
                entry["response"] = response
            if metadata:
                entry["metadata"].update(metadata)  # merge
            entry["updated_at"] = datetime.now(timezone.utc).isoformat()
            self._push_prompt_id(prompt_id)         # bump recency

    def get_recent_prompts(self, limit: int = 50, offset: int = 0) -> List[Dict[str, Any]]:
        """Return most recent prompts first."""
        with self._lock:
            ids = list(self._prompt_order)  # oldest -> newest
            ids.reverse()                   # newest -> oldest
            sliced = ids[offset:offset + limit]
            return [self._prompt_store[i] for i in sliced if i in self._prompt_store]

    # internal: maintain prompt recency & enforce capacity
    def _push_prompt_id(self, prompt_id: str):
        try:
            self._prompt_order.remove(prompt_id)  # remove if exists (bump)
        except ValueError:
            pass
        self._prompt_order.append(prompt_id)
        # enforce capacity
        while len(self._prompt_order) > self._prompt_max:
            oldest_id = self._prompt_order.popleft()
            self._prompt_store.pop(oldest_id, None)

    # =======================
    # Tasks (same pattern)
    # =======================
    def add_task(self, task_id: str, task_type: str, metadata: Optional[Dict[str, Any]] = None) -> None:
        with self._lock:
            entry = {
                "task_id": task_id,
                "task_type": task_type,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "metadata": metadata or {},
            }
            self._task_store[task_id] = entry
            self._push_task_id(task_id)
            self.stats["total_tasks"] += 1

    def get_recent_tasks(self, limit: int = 50, offset: int = 0) -> List[Dict[str, Any]]:
        with self._lock:
            ids = list(self._task_order)   # oldest -> newest
            ids.reverse()                  # newest -> oldest
            sliced = ids[offset:offset + limit]
            return [self._task_store[i] for i in sliced if i in self._task_store]

    def _push_task_id(self, task_id: str):
        try:
            self._task_order.remove(task_id)
        except ValueError:
            pass
        self._task_order.append(task_id)
        while len(self._task_order) > self._task_max:
            oldest_id = self._task_order.popleft()
            self._task_store.pop(oldest_id, None)

    # =======================
    # Stats
    # =======================
    def get_stats(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "prompt_buffer": {
                    "current_entries": len(self._prompt_store),
                    "max_size": self._prompt_max,
                    "total_entries": self.stats["total_prompts"],
                },
                "task_buffer": {
                    "current_entries": len(self._task_store),
                    "max_size": self._task_max,
                    "total_entries": self.stats["total_tasks"],
                },
                "active_users": {
                    "current_count": len(self.active_users),
                    "total_users": self.stats["total_users"],
                },
            }


# Optional singleton for easy import:
buffer_manager = OrchestratorBufferManager()

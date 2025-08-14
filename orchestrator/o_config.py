# orchestrator/o_config.py

import json
import os
import tempfile
import threading
import time
from datetime import datetime, timezone
from typing import Any, Dict
from typing import Optional  

# Best-effort POSIX file locking (no-op on non-POSIX)
try:
    import fcntl  # type: ignore
    _HAVE_FCNTL = True
except Exception:  # pragma: no cover
    fcntl = None
    _HAVE_FCNTL = False

# Allow override via env; falls back to project root file
_DEFAULT_PATH = os.getenv("ORCHESTRATOR_CONFIG_PATH", "orchestrator_config.json")
_DEFAULT_ORCH_ID = os.getenv("ORCHESTRATOR_ID", "orch-001")


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class OrchestratorConfigManager:
    """
    Thread-safe configuration manager for the Orchestrator.
    - In-process RW protected via threading.RLock
    - Best-effort file locking (fcntl on POSIX)
    - Atomic writes (temp file + os.replace)
    """

    def __init__(self, config_file: str = _DEFAULT_PATH):
        self.config_file = config_file
        self._rw = threading.RLock()
        self._file_lock = threading.Lock()  # serialize readers/writers across threads
        self._cfg: Dict[str, Any] = {}
        self._load()
        self._ensure_handshake_fields()

    # -------------------------
    # File lock helpers
    # -------------------------
    def _acquire_file_lock(self, fh, timeout_sec: int = 5) -> bool:
        """Acquire an exclusive file lock (POSIX). Returns True on success."""
        if not _HAVE_FCNTL:
            return True  # on non-POSIX, just continue
        deadline = time.time() + timeout_sec
        while time.time() < deadline:
            try:
                fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)  # type: ignore
                return True
            except Exception:
                time.sleep(0.05)
        return False

    def _release_file_lock(self, fh) -> None:
        if not _HAVE_FCNTL:
            return
        try:
            fcntl.flock(fh.fileno(), fcntl.LOCK_UN)  # type: ignore
        except Exception:
            pass

    # -------------------------
    # Load / Save
    # -------------------------
    def _load(self) -> Dict[str, Any]:
        """Load configuration from disk with best-effort file lock."""
        with self._file_lock:
            if not os.path.exists(self.config_file):
                cfg = self._default_config()
                self._save(cfg)
                self._cfg = cfg
                print(f"[O-CONFIG] Created default configuration at {self.config_file}")
                return cfg

            try:
                with open(self.config_file, "r", encoding="utf-8") as f:
                    if self._acquire_file_lock(f):
                        try:
                            data = f.read().strip()
                            if not data:
                                print(f"[O-CONFIG] {self.config_file} empty; using defaults")
                                self._cfg = self._default_config()
                                return self._cfg
                            self._cfg = json.loads(data)
                            print(f"[O-CONFIG] Loaded configuration from {self.config_file}")
                            return self._cfg
                        finally:
                            self._release_file_lock(f)
                    else:
                        print(f"[O-CONFIG] Lock timeout reading {self.config_file}; using in-memory")
                        return self._cfg or self._default_config()
            except Exception as e:
                print(f"[O-CONFIG] Error loading config: {e}; using defaults")
                self._cfg = self._default_config()
                return self._cfg

    def _save(self, config: Dict[str, Any]) -> None:
        """Atomic write with best-effort file lock."""
        with self._file_lock:
            # ensure metadata & timestamps
            config.setdefault("metadata", {})
            config["metadata"]["last_updated"] = _utc_now_iso()

            os.makedirs(os.path.dirname(self.config_file) or ".", exist_ok=True)

            # Use NamedTemporaryFile to ensure same filesystem; then replace
            with tempfile.NamedTemporaryFile(
                mode="w",
                encoding="utf-8",
                dir=os.path.dirname(self.config_file) or ".",
                prefix=os.path.basename(self.config_file) + ".",
                suffix=".tmp",
                delete=False,
            ) as tf:
                temp_path = tf.name
                if self._acquire_file_lock(tf):
                    try:
                        json.dump(config, tf, indent=2, sort_keys=True)
                        tf.flush()
                        os.fsync(tf.fileno())
                    finally:
                        self._release_file_lock(tf)
                else:
                    # If we can't lock temp file, still attempt to write atomically
                    json.dump(config, tf, indent=2, sort_keys=True)
                    tf.flush()
                    os.fsync(tf.fileno())

            # os.replace is atomic on POSIX/NTFS
            os.replace(temp_path, self.config_file)
            print(f"[O-CONFIG] Configuration saved to {self.config_file}")

    # -------------------------
    # Defaults / Structure
    # -------------------------
    def _default_config(self) -> Dict[str, Any]:
        return {
            "service_type": "orchestrator",
            "orchestrator_id": _DEFAULT_ORCH_ID,
            "organization": {
                "name": "Default Orchestrator",
                "location": "localhost",
            },
            "features": {
                "firewall": False,
                "cache": False,
                "llm": True,
                "prompt_response_agent": True,
                "evaluation_agent": False,
                "prompt_domain_identifier_agent": False,
                "task_agent": False,
                "agentic_rag_agent": False,
            },
            "firewall": {
                "enabled": False,
                "blocklist": ["malicious.com", "spam.net", "phishing.org"],
                "blocked_keywords": ["hack", "exploit", "malware", "virus", "attack"],
                "max_prompt_length": 10000,
                "rate_limit_per_minute": 60,
            },
            "cache": {
                "enabled": False,
                "max_size": 1000,
                "ttl_seconds": 3600,
                "cleanup_interval_seconds": 300,
            },
            "llm": {
                "enabled": True,
                "model": "gpt-3.5-turbo",
                "max_tokens": 1000,
                "temperature": 0.7,
                "timeout_seconds": 30,
            },
            "websocket": {
                "controller_host": "localhost",
                "controller_port": 8765,
                "keepalive_interval": 10,
                "reconnect_delay": 5,
                "max_reconnect_attempts": 10,
            },
            "gui": {
                "orchestrator_port": 8001,
                "cors_origins": ["*"],
                "request_timeout_seconds": 30,
            },
            "metadata": {
                "version": "1.0.0",
                "created": _utc_now_iso(),
                "last_updated": _utc_now_iso(),
                "capabilities": [],
                "ssl_enabled": False,
            },
        }

    def _ensure_handshake_fields(self) -> None:
        with self._rw:
            c = self._cfg
            c.setdefault("orchestrator_id", _DEFAULT_ORCH_ID)
            c.setdefault("organization", {})
            c["organization"].setdefault("name", "Unknown Orchestrator")
            c["organization"].setdefault("location", "Unknown")
            c.setdefault("metadata", {})
            c["metadata"].setdefault("version", "1.0.0")
            c["metadata"].setdefault("last_updated", _utc_now_iso())
            c["metadata"].setdefault("capabilities", [])
            c["metadata"].setdefault("ssl_enabled", False)

    # -------------------------
    # Public API (thread-safe)
    # -------------------------
    def get_config(self) -> Dict[str, Any]:
        with self._rw:
            return dict(self._cfg)

    def update_config(self, new_config: Dict[str, Any]) -> None:
        """Replace the full config with a new dict and persist."""
        with self._rw:
            self._cfg = dict(new_config or {})
            self._ensure_handshake_fields()
            self._save(self._cfg)

    def patch_config(self, patch: Dict[str, Any]) -> Dict[str, Any]:
        """
        Shallow merge update (useful for toggling features/params)
        and persist. Returns the updated config.
        """
        with self._rw:
            base = dict(self._cfg)
            for k, v in (patch or {}).items():
                if isinstance(v, dict) and isinstance(base.get(k), dict):
                    base[k].update(v)
                else:
                    base[k] = v
            self._cfg = base
            self._ensure_handshake_fields()
            self._save(self._cfg)
            return dict(self._cfg)

    def reload_config(self) -> Dict[str, Any]:
        with self._rw:
            self._load()
            self._ensure_handshake_fields()
            print(f"[O-CONFIG] Configuration reloaded from {self.config_file}")
            return dict(self._cfg)

    # -------------------------
    # Convenience helpers
    # -------------------------
    def get_feature(self, name: str) -> bool:
        with self._rw:
            return bool(self._cfg.get("features", {}).get(name, False))

    def set_feature(self, name: str, enabled: bool) -> Dict[str, Any]:
        with self._rw:
            self._cfg.setdefault("features", {})
            self._cfg["features"][name] = bool(enabled)
            self._save(self._cfg)
            print(f"[O-CONFIG] Feature {name} set to {enabled}")
            return dict(self._cfg)

    def update_features(self, features: Dict[str, Any]) -> Dict[str, Any]:
        """Bulk update features (dict of name->value)."""
        return self.patch_config({"features": dict(features or {})})

    def touch_presence(self, status: Optional[str] = None) -> Dict[str, Any]:
        """
        Update presence-related fields; handy for keepalives, etc.
        """
        with self._rw:
            if status is not None:
                self._cfg["status"] = status
            self._cfg["last_seen"] = _utc_now_iso()
            self._save(self._cfg)
            return dict(self._cfg)


# Global instance for orchestrator
import os
orchestrator_config = OrchestratorConfigManager(
    os.getenv("ORCHESTRATOR_CONFIG_FILE", "orchestrator_config.json")
)


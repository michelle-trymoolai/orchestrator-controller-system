import json
import threading
import os
import fcntl
import tempfile
from typing import Dict, Any
from datetime import datetime
import time


class ControllerConfigManager:
    """Thread-safe configuration manager for Controller with file locking."""
    
    def __init__(self, config_file: str = "controller_config.json"):
        self.config_file = config_file
        self._lock = threading.RLock()
        self._file_lock = threading.Lock()
        self._config = self._load_config()
    
    def _acquire_file_lock(self, file_handle, timeout: int = 5):
        """Acquire file lock with timeout."""
        start_time = time.time()
        while time.time() - start_time < timeout:
            try:
                fcntl.flock(file_handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                return True
            except (IOError, OSError):
                time.sleep(0.1)
        return False
    
    def _release_file_lock(self, file_handle):
        """Release file lock."""
        try:
            fcntl.flock(file_handle.fileno(), fcntl.LOCK_UN)
        except (IOError, OSError):
            pass
    
    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from file with file locking."""
        with self._file_lock:
            try:
                if os.path.exists(self.config_file):
                    with open(self.config_file, 'r') as f:
                        if self._acquire_file_lock(f):
                            try:
                                config = json.load(f)
                                print(f"[C-CONFIG] Loaded configuration from {self.config_file}")
                                return config
                            finally:
                                self._release_file_lock(f)
                        else:
                            print(f"[C-CONFIG] Could not acquire lock for {self.config_file}, using defaults")
                            return self._get_default_config()
                else:
                    config = self._get_default_config()
                    self._save_config(config)
                    print(f"[C-CONFIG] Created default configuration at {self.config_file}")
                    return config
            except Exception as e:
                print(f"[C-CONFIG] Error loading config: {e}, using defaults")
                return self._get_default_config()
    
    def _get_default_config(self) -> Dict[str, Any]:
        """Get default controller configuration."""
        return {
            "service_type": "controller",
            "organizations": {
                "orch-001": {
                    "name": "Default Organization",
                    "location": "localhost",
                    "features": {
                        "firewall": False,
                        "cache": False,
                        "llm": True,
                        "prompt_response_agent": True,
                        "evaluation_agent": False,
                        "prompt_domain_identifier_agent": False,
                        "task_agent": False,
                        "agentic_rag_agent": False
                    },
                    "status": "active",
                    "last_seen": None
                }
            },
            "websocket": {
                "controller_host": "localhost",
                "controller_port": 8765,
                "keepalive_interval": 10
            },
            "gui": {
                "controller_port": 8002,
                "cors_origins": ["*"],
                "request_timeout_seconds": 30
            },
            "metadata": {
                "version": "1.0.0",
                "created": datetime.now().isoformat(),
                "last_updated": datetime.now().isoformat()
            }
        }
    
    def _save_config(self, config: Dict[str, Any]) -> None:
        """Save configuration to file with atomic write and file locking."""
        with self._file_lock:
            try:
                config["metadata"]["last_updated"] = datetime.now().isoformat()
                
                temp_fd, temp_path = tempfile.mkstemp(
                    suffix='.tmp',
                    prefix=os.path.basename(self.config_file) + '.',
                    dir=os.path.dirname(self.config_file) or '.'
                )
                
                try:
                    with os.fdopen(temp_fd, 'w') as temp_file:
                        if self._acquire_file_lock(temp_file):
                            try:
                                json.dump(config, temp_file, indent=2, sort_keys=True)
                                temp_file.flush()
                                os.fsync(temp_file.fileno())
                            finally:
                                self._release_file_lock(temp_file)
                        else:
                            raise Exception("Could not acquire file lock for writing")
                    
                    if os.name == 'nt':
                        if os.path.exists(self.config_file):
                            os.remove(self.config_file)
                    os.rename(temp_path, self.config_file)
                    
                except Exception:
                    try:
                        os.unlink(temp_path)
                    except OSError:
                        pass
                    raise
                    
            except Exception as e:
                print(f"[C-CONFIG] Error saving config: {e}")
                raise
    
    def get_config(self) -> Dict[str, Any]:
        """Get current configuration (thread-safe)."""
        with self._lock:
            return self._config.copy()
    
    def update_config(self, new_config: Dict[str, Any]) -> None:
        """Update configuration (thread-safe)."""
        with self._lock:
            self._config = new_config.copy()
            self._save_config(self._config)
    
    def get_feature(self, feature_name: str, orch_id: str) -> bool:
        """Get specific feature status for an orchestrator."""
        with self._lock:
            return self._config.get("organizations", {}).get(orch_id, {}).get("features", {}).get(feature_name, False)
    
    def set_feature(self, feature_name: str, enabled: bool, orch_id: str) -> None:
        """Set specific feature status for an orchestrator."""
        with self._lock:
            if "organizations" not in self._config:
                self._config["organizations"] = {}
            if orch_id not in self._config["organizations"]:
                self._config["organizations"][orch_id] = {"features": {}}
            if "features" not in self._config["organizations"][orch_id]:
                self._config["organizations"][orch_id]["features"] = {}
            self._config["organizations"][orch_id]["features"][feature_name] = enabled
            self._save_config(self._config)
    
    def register_orchestrator(self, orch_id: str, info: Dict[str, Any] = None) -> None:
        """Register or update an orchestrator record safely."""
        info = info or {}
        now = datetime.now().isoformat()

        default_features = {
            "firewall": False,
            "cache": False,
            "llm": True,
            "prompt_response_agent": True,
            "evaluation_agent": False,
            "prompt_domain_identifier_agent": False,
            "task_agent": False,
            "agentic_rag_agent": False,
        }

        with self._lock:
            cfg = self._config
            orgs = cfg.setdefault("organizations", {})
            org = orgs.setdefault(orch_id, {
                "name": f"Organization {orch_id}",
                "location": "unknown",
                "features": default_features,
                "status": "active",
                "last_seen": now,
                "metadata": {},
            })

            org["status"] = "active"
            org["last_seen"] = now

            if "name" in info:
                org["name"] = info["name"]
            if "location" in info:
                org["location"] = info["location"]

            org.setdefault("metadata", {})
            md = info.get("metadata")
            if isinstance(md, dict):
                org["metadata"].update(md)
            else:
                for k, v in info.items():
                    if k not in {"name", "location"}:
                        org["metadata"][k] = v

            self._save_config(cfg)
    
    def unregister_orchestrator(self, orch_id: str) -> None:
        """Unregister an orchestrator."""
        with self._lock:
            if "organizations" in self._config and orch_id in self._config["organizations"]:
                self._config["organizations"][orch_id]["status"] = "inactive"
                self._config["organizations"][orch_id]["last_seen"] = datetime.now().isoformat()
                self._save_config(self._config)

    def reload_config(self) -> None:
        """Reload configuration from file."""
        with self._lock:
            self._config = self._load_config()
            print(f"[C-CONFIG] Configuration reloaded from {self.config_file}")


# Global instance for controller
controller_config = ControllerConfigManager("controller_config.json")

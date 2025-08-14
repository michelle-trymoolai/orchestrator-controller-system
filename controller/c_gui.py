# controller/c_gui.py
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import uvicorn
import os
import json
import urllib.request
import urllib.error
import time

from controller.c_config import controller_config
from controller.utils.controller_state import list_orchestrators, get_ws
from controller.utils.provisioning import send_provisioning_to
from controller.utils.buffer_manager import controller_buffers  # shared singleton

app = FastAPI(title="Controller GUI")

# ---------- Models ----------
class OrchestratorCreate(BaseModel):
    org_id: str
    name: str
    location: str
    metadata: dict = {}
    features: dict = {}

class FeatureUpdate(BaseModel):
    features: dict

# ---------- Small helper to call C-OCS internal HTTP ----------
OCS_HTTP_HOST = os.getenv("CONTROLLER_OCS_HTTP_HOST", "localhost")
OCS_HTTP_PORT = int(os.getenv("CONTROLLER_OCS_HTTP_PORT", "8010"))

def _ocx_get(path: str, params: str = ""):
    url = f"http://{OCS_HTTP_HOST}:{OCS_HTTP_PORT}{path}{params}"
    try:
        with urllib.request.urlopen(url, timeout=5) as resp:
            body = resp.read().decode("utf-8")
            return json.loads(body)
    except urllib.error.HTTPError as e:
        try:
            detail = e.read().decode("utf-8")
        except Exception:
            detail = str(e)
        raise HTTPException(status_code=e.code, detail=detail)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"OCS proxy error: {e}")

# ---------- Config-backed org CRUD ----------
@app.get("/organizations")
def list_orgs():
    # ensure we read the freshest presence/status persisted by C-OCS
    controller_config.reload_config()
    return controller_config.get_config().get("organizations", {})

@app.get("/organizations/{org_id}")
def get_org(org_id: str):
    controller_config.reload_config()
    orgs = controller_config.get_config().get("organizations", {})
    if org_id not in orgs:
        raise HTTPException(404, "Organization not found")
    return orgs[org_id]

@app.post("/organizations")
def create_org(org: OrchestratorCreate):
    cfg = controller_config.get_config()
    orgs = cfg.setdefault("organizations", {})
    if org.org_id in orgs:
        raise HTTPException(400, "Organization ID already exists")
    orgs[org.org_id] = {
        "name": org.name,
        "location": org.location,
        "metadata": org.metadata,
        "features": org.features,
        "last_seen": None,
        "status": "inactive",
    }
    controller_config.update_config(cfg)
    return {"status": "created", "org_id": org.org_id}

@app.put("/organizations/{org_id}/features")
def update_features(org_id: str, update: FeatureUpdate):
    cfg = controller_config.get_config()
    if org_id not in cfg.get("organizations", {}):
        raise HTTPException(404, "Organization not found")
    cfg["organizations"][org_id]["features"].update(update.features)
    controller_config.update_config(cfg)
    # C-OCS watcher will detect and push.
    return {"status": "ok", "message": "Features updated; provisioning will be pushed shortly"}

# ---------- Live orchestrators ----------
@app.get("/orchestrators")
def live_orchestrators():
    """Live snapshot via C-OCS proxy (so this never shows {})."""
    return orchestrators_live()

@app.get("/orchestrators/{org_id}")
def live_orchestrator(org_id: str):
    snapshot = orchestrators_live()
    if org_id not in snapshot:
        raise HTTPException(404, "Orchestrator not connected")
    return snapshot[org_id]

# ---------- Force provisioning push ----------
@app.post("/provision/{org_id}")
async def force_provision(org_id: str):
    ws = get_ws(org_id)
    if ws is None:
        raise HTTPException(404, "Orchestrator not connected")
    await send_provisioning_to(ws, org_id, controller_config)
    controller_buffers.add_activity("provisioning_forced", {"orch_id": org_id})
    return {"status": "sent", "org_id": org_id}

# ---------- Existing: Buffer stats + activities (THIS process) ----------
@app.get("/buffer_stats")
def buffer_stats():
    return controller_buffers.get_stats()

@app.get("/activities")
def activities(limit: int = 50, offset: int = 0):
    return controller_buffers.get_recent_activities(limit=limit, offset=offset)

# ---------- Proxies to C-OCS internal HTTP (live data from that process) ----------
@app.get("/orchestrators_live")
def orchestrators_live():
    return _ocx_get("/ocx/orchestrators")

@app.get("/controller_buffer_stats")
def controller_buffer_stats():
    return _ocx_get("/ocx/buffer_stats")


@app.get("/healthz")
def healthz():
    cfg = controller_config.get_config()
    version = ((cfg or {}).get("metadata") or {}).get("version", "unknown")
    return {
        "ok": True,
        "service": "c-gui",
        "version": version,
        "ts": time.time(),
    }


@app.get("/controller_activities")
def controller_activities(limit: int = 50, offset: int = 0):
    qs = f"?limit={int(limit)}&offset={int(offset)}"
    return _ocx_get("/ocx/activities", qs)

@app.post("/login")
def login():
    return {"status": "logged_in", "user": "admin"}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8002)

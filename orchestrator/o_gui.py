from fastapi import FastAPI, HTTPException
from fastapi import Query
from fastapi import Request
from pydantic import BaseModel
import uvicorn
import os, asyncio, time, uuid, json
from orchestrator.o_config import orchestrator_config

# buffer manager
try:
    from orchestrator.utils.buffer_manager import OrchestratorBufferManager, buffer_manager
except Exception:
    OrchestratorBufferManager = None
    buffer_manager = None

app = FastAPI(title="O-GUI")

ORCHESTRATOR_ID = os.getenv("ORCHESTRATOR_ID", "orch-001")
ORCH_HTTP_PORT = int(os.getenv("ORCHESTRATOR_HTTP_PORT", "8011"))  # where O-CCS listens

# --- models ---
class PromptIn(BaseModel):
    user_id: str = "demo-user"
    prompt: str

class FeatureUpdate(BaseModel):
    features: dict

class LoginIn(BaseModel):
    user_id: str = "orch_admin"


# ---------- helpers ----------

async def _push_config_async():
    try:
        # httpx 
        try:
            import httpx
            async with httpx.AsyncClient(timeout=3) as client:
                await client.post("http://localhost:8011/push_config")
            return
        except Exception:
            pass
        # fallback: 
        import urllib.request, urllib.error, asyncio
        def _sync_push():
            req = urllib.request.Request(
                "http://localhost:8011/push_config",
                method="POST",
                data=b""
            )
            with urllib.request.urlopen(req, timeout=3) as _:
                _ .read()
        await asyncio.to_thread(_sync_push)
    except Exception as e:
        print(f"[O-GUI] push_config failed: {e}")

async def _active_user_cleanup_loop():
    """Background: purge stale active users every 60s (30 min idle threshold)."""
    if not buffer_manager:
        return
    while True:
        try:
            buffer_manager.cleanup_expired(timeout_seconds=1800)
        except Exception as e:
            print(f"[O-GUI] cleanup_expired error: {e}")
        await asyncio.sleep(60)


@app.on_event("startup")
async def _startup_bg_tasks():
    asyncio.create_task(_active_user_cleanup_loop())


# --- simple status + config  ---
@app.get("/healthz")
def healthz():
    cfg = orchestrator_config.get_config()
    version = ((cfg or {}).get("metadata") or {}).get("version", "unknown")
    return {
        "ok": True,
        "service": "o-gui",
        "orchestrator_id": ORCHESTRATOR_ID,
        "version": version,
        "ts": time.time(),
    }

@app.get("/status")
def status():
    cfg = orchestrator_config.get_config()
    return {
        "orchestrator_id": ORCHESTRATOR_ID,
        "ok": True,
        "ts": time.time(),
        "cfg_status": cfg.get("status", "unknown"),
        "last_seen": cfg.get("last_seen"),
    }

@app.get("/config")
def get_cfg():
    return orchestrator_config.get_config()

# --- feature toggles  ---
@app.post("/features")
async def update_features(update: FeatureUpdate):
    cfg = orchestrator_config.get_config()
    if "features" not in cfg:
        cfg["features"] = {}
    cfg["features"].update(update.features)
    orchestrator_config.update_config(cfg)
    # auto-push the new O-Config snapshot upstream (non-blocking)
    asyncio.create_task(_push_config_async())
    return {"status": "updated", "features": cfg["features"]}


@app.post("/login")
def login(body: LoginIn):
    if buffer_manager:
        buffer_manager.update_active_user(
            user_id=body.user_id, orch_id=ORCHESTRATOR_ID, metadata={"via": "o_gui"}
        )
    return {"status": "logged_in", "user": body.user_id}

# --- prompt processing with child task + buffers ---


def _simulate_agent_response(prompt: str) -> dict:
    return {
        "text": f"[demo] reply to: {prompt}",
        "tokens_in": len(prompt.split()),
        "tokens_out": 8,
        "latency_ms": 1200,
    }

@app.post("/process_prompt")
async def process_prompt(p: PromptIn, request: Request):
    """
    Accept a prompt, create a prompt_id, store it in the buffer,
    kick off a background task, and (optionally) return a synchronous
    simulated response when ?simulate=1 is provided.
    You can also hold the HTTP socket open via ?delay=SECONDS.
    """
    prompt_id = str(uuid.uuid4())
    meta = {"orchestrator_id": ORCHESTRATOR_ID, "received_ts": time.time()}

    # record receipt
    if buffer_manager:
        try:
            buffer_manager.add_prompt(
                prompt_id=prompt_id,
                user_id=p.user_id,
                prompt=p.prompt,
                response=None,
                metadata=meta,
            )
        except Exception as e:
            print(f"[O-GUI] buffer add_prompt error: {e}")

    # always keep background behavior
    asyncio.create_task(_run_agent(prompt_id, p.user_id, p.prompt, meta))

    # keeping connection open for netstat
    try:
        raw_delay = request.query_params.get("delay", "0")
        delay = max(0.0, min(10.0, float(raw_delay)))
        if delay:
            print(f"[O-GUI] holding connection {delay:.1f}s (prompt_id={prompt_id})")
            await asyncio.sleep(delay)
    except ValueError:
        raise HTTPException(status_code=400, detail="delay must be a number")

    # synchronous simulated reply
    simulate_flag = request.query_params.get("simulate", "0").lower()
    simulate = simulate_flag in ("1", "true", "yes")
    if simulate:
        # small think-time
        await asyncio.sleep(0.2)
        sim = _simulate_agent_response(p.prompt)

        # write back for pollers 
        try:
            if buffer_manager and hasattr(buffer_manager, "update_prompt_response"):
                buffer_manager.update_prompt_response(prompt_id, sim)
        except Exception as e:
            print(f"[O-GUI] buffer update error: {e}")

        print(f"[O-GUI] returning simulated response (prompt_id={prompt_id})")
        return {"prompt_id": prompt_id, "status": "accepted", "response": sim}

    # default immediate ack
    return {"prompt_id": prompt_id, "status": "accepted"}



@app.get("/prompt/{prompt_id}")
def get_prompt(prompt_id: str):
    if not buffer_manager:
        raise HTTPException(500, "buffer manager not available")
    item = buffer_manager.get_prompt(prompt_id)
    if not item:
        raise HTTPException(404, "unknown prompt_id")
    return item

@app.get("/recent_prompts")
def recent_prompts(limit: int = Query(50, ge=1, le=500), offset: int = Query(0, ge=0)):
    if not buffer_manager:
        raise HTTPException(500, "buffer manager not available")
    return buffer_manager.get_recent_prompts(limit=limit, offset=offset)

@app.get("/buffer_stats")
def buffer_stats():
    if not buffer_manager:
        raise HTTPException(500, "buffer manager not available")
    return buffer_manager.get_stats()

@app.get("/recent_tasks")
def recent_tasks(limit: int = Query(50, ge=1, le=500), offset: int = Query(0, ge=0)):
    if not buffer_manager:
        raise HTTPException(500, "buffer manager not available")
    return buffer_manager.get_recent_tasks(limit=limit, offset=offset)

@app.get("/active_users")
def active_users():
    if not buffer_manager:
        raise HTTPException(500, "buffer manager not available")
    return buffer_manager.get_active_users()

@app.get("/active_users/{user_id}")
def active_user(user_id: str):
    if not buffer_manager:
        raise HTTPException(500, "buffer manager not available")
    item = buffer_manager.get_active_user(user_id)
    if not item:
        raise HTTPException(404, "unknown user_id")
    return item

# --- background 'agent' task  ---
async def _run_agent(prompt_id: str, user_id: str, prompt: str, meta: dict):
    try:
        await asyncio.sleep(0.5)
        fake_response = f"[{meta['orchestrator_id']}] Echo: {prompt}"

        if buffer_manager:
            buffer_manager.update_prompt(
                prompt_id=prompt_id,
                response=fake_response,
                metadata={**meta, "done": True, "completed_ts": time.time()},
            )
        print(f"[O-GUI:{meta['orchestrator_id']}] prompt_id={prompt_id} DONE")
    except Exception as e:
        print(f"[O-GUI] _run_agent error: {e}")

# --- run server ---
if __name__ == "__main__":
    uvicorn.run(
        app,
        host=os.getenv("ORCHESTRATOR_GUI_HOST", "0.0.0.0"),
        port=int(os.getenv("ORCHESTRATOR_GUI_PORT", "8001"))
    )

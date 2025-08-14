# controller/utils/provisioning.py
import json

async def send_provisioning_to(ws, orch_id: str, controller_config):
    """
    Push updated provisioning to a specific orchestrator websocket.
    Payload includes orchestrator_id and the minimal fields O-CCS needs.
    """
    cfg = controller_config.get_config()
    orgs = cfg.get("organizations", {})
    org = orgs.get(orch_id)
    if not org:
        print(f"[C-OCS] No such org_id {orch_id} to provision")
        return

    # Send only what the orchestrator needs (features + metadata + name/location),
    # AND include orchestrator_id explicitly.
    msg = {
        "type": "provisioning",
        "data": {
            "orchestrator_id": orch_id,
            "features": org.get("features", {}),
            "metadata": org.get("metadata", {}),
            "name": org.get("name"),
            "location": org.get("location"),
        }
    }
    try:
        await ws.send(json.dumps(msg))
        print(f"[C-OCS] Provisioning sent to {orch_id}: {msg['data']['features']}")
    except Exception as e:
        print(f"[C-OCS] Failed to send provisioning to {orch_id}: {e}")


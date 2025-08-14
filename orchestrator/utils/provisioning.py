# orchestrator/utils/provisioning.py
def apply_provisioning(data: dict, orchestrator_config):
    """
    Applies provisioning data from the controller to local orchestrator config.
    Requires:
      data["orchestrator_id"]
      data["features"] (dict)
    Optional:
      data["metadata"] (dict), data["name"], data["location"]
    """
    org_id = data.get("orchestrator_id")
    if not org_id:
        print("[O-CCS] Provisioning missing 'orchestrator_id' — ignoring")
        return

    features = data.get("features", {})
    metadata = data.get("metadata", {})
    name = data.get("name")
    location = data.get("location")

    cfg = orchestrator_config.get_config()
    orgs = cfg.setdefault("organizations", {})
    org = orgs.setdefault(org_id, {})

    # ensure nested dicts
    org_features = org.setdefault("features", {})
    org_meta = org.setdefault("metadata", {})

    # update slices
    org_features.clear()
    org_features.update(features)
    org_meta.update(metadata or {})

    if name:
        org["name"] = name
    if location:
        org["location"] = location

    orchestrator_config.update_config(cfg)
    print(f"[O-CCS] Provisioning applied for {org_id}: {features}")

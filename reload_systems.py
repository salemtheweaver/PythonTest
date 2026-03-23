def reload_systems():
    """Reload all systems from disk and GitHub."""
    import os
    import json
    from config import JSON_FILE, GITHUB_TOKEN
    global systems_data
    systems_data = {"systems": {}}
    if GITHUB_TOKEN:
        from data import _github_load_all_systems, _github_get_file
        split_systems = _github_load_all_systems()
        if split_systems:
            systems_data["systems"] = split_systems
        mono_data, _ = _github_get_file(JSON_FILE)
        if mono_data and mono_data.get("_moderation"):
            systems_data["_moderation"] = mono_data["_moderation"]
        if mono_data and mono_data.get("systems"):
            for sys_id, sys_data in mono_data["systems"].items():
                if sys_id not in systems_data["systems"]:
                    systems_data["systems"][sys_id] = sys_data
    if os.path.exists(JSON_FILE):
        with open(JSON_FILE, "r") as f:
            local_data = json.load(f)
        if local_data.get("_moderation"):
            systems_data.setdefault("_moderation", {}).update(local_data["_moderation"])
        if not systems_data["systems"] and local_data.get("systems"):
            systems_data["systems"] = local_data["systems"]
    return systems_data

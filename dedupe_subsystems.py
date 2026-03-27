import os
import json
from collections import defaultdict

SYSTEMS_DIR = "systems"

def load_system(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def save_system(path, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

def dedupe_subsystems(system):
    changed = False
    report = []
    # Main system subsystems
    subsystems = system.get("subsystems", {})
    seen_names = {}
    to_delete = []
    for sub_id, sub in subsystems.items():
        name = (sub.get("subsystem_name") or "").strip().lower()
        if name and name in seen_names:
            to_delete.append(sub_id)
            report.append(f"Duplicate main subsystem: '{sub.get('subsystem_name')}' (ID: {sub_id})")
        else:
            seen_names[name] = sub_id
    for sub_id in to_delete:
        del subsystems[sub_id]
        changed = True
    # Side system subsystems
    for side_id, side in (system.get("side_systems", {}) or {}).items():
        subs = side.get("subsystems", {})
        seen_names = {}
        to_delete = []
        for sub_id, sub in subs.items():
            name = (sub.get("subsystem_name") or "").strip().lower()
            if name and name in seen_names:
                to_delete.append(sub_id)
                report.append(f"Duplicate in side '{side_id}': '{sub.get('subsystem_name')}' (ID: {sub_id})")
            else:
                seen_names[name] = sub_id
        for sub_id in to_delete:
            del subs[sub_id]
            changed = True
    return changed, report

def main():
    summary = []
    for fname in os.listdir(SYSTEMS_DIR):
        if not fname.endswith(".json"): continue
        path = os.path.join(SYSTEMS_DIR, fname)
        system = load_system(path)
        changed, report = dedupe_subsystems(system)
        if changed:
            save_system(path, system)
            summary.append(f"{fname}:\n  " + "\n  ".join(report))
    if summary:
        print("Duplicates removed from the following systems:")
        print("\n\n".join(summary))
    else:
        print("No duplicate subsystems found.")
    print("\n--- Investigation ---")
    print("Duplicates usually appear due to import bugs or repeated creation commands. If you recently imported data or used /createsubsystem multiple times quickly, this may be the cause. Check import logic and command handling for ID assignment and name checks.")

if __name__ == "__main__":
    main()

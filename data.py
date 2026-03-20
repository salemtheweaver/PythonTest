# data.py — Data persistence, GitHub helpers, and systems_data

import os
import sys
import json
import base64
import signal
import threading
import urllib.request
import urllib.error

from config import JSON_FILE, GITHUB_TOKEN, GITHUB_REPO

SYSTEMS_DIR = "systems"  # GitHub directory for per-system files


_save_worker_lock = threading.Lock()
_save_worker_started = False
_save_condition = threading.Condition(_save_worker_lock)
_pending_save_payload = None


def _background_github_save_worker():
    """Serialize GitHub saves so command handlers are not blocked by network I/O."""
    global _pending_save_payload

    while True:
        with _save_condition:
            while _pending_save_payload is None:
                _save_condition.wait()
            payload = _pending_save_payload
            _pending_save_payload = None

        # Save each system as its own small file instead of one monolith
        for sys_id, sys_data in payload.get("systems", {}).items():
            _github_save_system(sys_id, sys_data)

        # Persist shared moderation/admin state so it survives restarts.
        _github_save_file(JSON_FILE, payload)


def _queue_github_save(data_obj):
    """Queue the latest data snapshot for background GitHub persistence."""
    global _pending_save_payload, _save_worker_started

    # Freeze the payload so later in-memory mutations do not leak into the save.
    payload = json.loads(json.dumps(data_obj))

    with _save_condition:
        _pending_save_payload = payload
        if not _save_worker_started:
            worker = threading.Thread(target=_background_github_save_worker, daemon=True)
            worker.start()
            _save_worker_started = True
        _save_condition.notify()


def _queue_system_save(system_id, system_data):
    """Queue a single system save for background GitHub persistence."""
    global _pending_save_payload, _save_worker_started

    payload = {"systems": {system_id: json.loads(json.dumps(system_data))}}

    with _save_condition:
        _pending_save_payload = payload
        if not _save_worker_started:
            worker = threading.Thread(target=_background_github_save_worker, daemon=True)
            worker.start()
            _save_worker_started = True
        _save_condition.notify()


def flush_pending_save():
    """Synchronously flush any queued GitHub save. Called on shutdown so in-flight
    saves are not lost when the process receives SIGTERM."""
    global _pending_save_payload
    with _save_condition:
        payload = _pending_save_payload
        _pending_save_payload = None
    if payload and GITHUB_TOKEN:
        print("[INFO] Flushing pending GitHub saves before shutdown...")
        for sys_id, sys_data in payload.get("systems", {}).items():
            _github_save_system(sys_id, sys_data)
        _github_save_file(JSON_FILE, payload)
        print("[INFO] Shutdown save complete.")


def _sigterm_handler(signum, frame):
    """Handle SIGTERM gracefully: flush pending saves then exit."""
    print("[INFO] SIGTERM received — flushing saves before exit.")
    flush_pending_save()
    sys.exit(0)


signal.signal(signal.SIGTERM, _sigterm_handler)


# --- GitHub persistence helpers ---
def _github_get_file(filename):
    """Get file content and sha from GitHub. Handles large files via download_url."""
    url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{filename}"
    req = urllib.request.Request(url, headers={
        "Authorization": f"token {GITHUB_TOKEN}",
        "Accept": "application/vnd.github.v3+json",
    })
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            meta = json.loads(resp.read().decode("utf-8"))
            sha = meta.get("sha")
            # Small files have inline content; large files only have download_url
            if meta.get("content"):
                content = base64.b64decode(meta["content"]).decode("utf-8")
                return json.loads(content), sha
            elif meta.get("download_url"):
                dl_req = urllib.request.Request(meta["download_url"], headers={
                    "Authorization": f"token {GITHUB_TOKEN}",
                })
                with urllib.request.urlopen(dl_req, timeout=30) as dl_resp:
                    content = dl_resp.read().decode("utf-8")
                    return json.loads(content), sha
    except Exception as e:
        print(f"[WARN] _github_get_file({filename}) failed: {e}")
    return None, None


def _github_list_dir(dirname):
    """List files in a GitHub directory. Returns list of {name, path} dicts."""
    url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{dirname}"
    req = urllib.request.Request(url, headers={
        "Authorization": f"token {GITHUB_TOKEN}",
        "Accept": "application/vnd.github.v3+json",
    })
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            items = json.loads(resp.read().decode("utf-8"))
            if isinstance(items, list):
                return [{"name": f["name"], "path": f["path"]} for f in items if f.get("type") == "file"]
    except Exception:
        pass
    return []


def _github_save_file(filename, data_obj, retries=6):
    """Save JSON data to GitHub repo with retry logic and compact encoding."""
    if not GITHUB_TOKEN:
        return
    json_bytes = json.dumps(data_obj, separators=(",", ":")).encode("utf-8")
    encoded = base64.b64encode(json_bytes).decode("utf-8")
    size_mb = len(json_bytes) / (1024 * 1024)
    if size_mb > 50:
        print(f"[WARN] {filename} is {size_mb:.1f}MB — consider splitting further")

    for attempt in range(1, retries + 1):
        try:
            _, sha = _github_get_file(filename)
            body = json.dumps({
                "message": f"Auto-update {filename}",
                "content": encoded,
                **({"sha": sha} if sha else {}),
            }).encode("utf-8")
            url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{filename}"
            req = urllib.request.Request(url, data=body, method="PUT", headers={
                "Authorization": f"token {GITHUB_TOKEN}",
                "Accept": "application/vnd.github.v3+json",
                "Content-Type": "application/json",
            })
            timeout = 30 + int(size_mb * 5)
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                resp.read()
            return  # success
        except urllib.error.HTTPError as e:
            print(f"[WARN] GitHub save attempt {attempt}/{retries} for {filename} failed: HTTP Error {e.code}: {e.reason}")
            if attempt < retries:
                import time
                # 409 Conflict means stale SHA — exponential backoff with jitter to avoid thundering herd
                if e.code == 409:
                    delay = min(60, 2 ** attempt)
                else:
                    delay = 2 * attempt
                time.sleep(delay)
        except Exception as e:
            print(f"[WARN] GitHub save attempt {attempt}/{retries} for {filename} failed: {e}")
            if attempt < retries:
                import time
                time.sleep(2 * attempt)
    print(f"[ERROR] All {retries} attempts to push {filename} to GitHub failed")


# --- Per-system GitHub storage ---
def _github_load_all_systems():
    """Load all systems from individual GitHub files in systems/ directory."""
    systems = {}
    files = _github_list_dir(SYSTEMS_DIR)
    for f in files:
        if f["name"].endswith(".json"):
            data, _ = _github_get_file(f["path"])
            if data and isinstance(data, dict):
                # Each file stores a single system keyed by its system_id
                for sys_id, sys_data in data.items():
                    systems[sys_id] = sys_data
    return systems


def _github_save_system(system_id, system_data):
    """Save a single system to its own file on GitHub."""
    filename = f"{SYSTEMS_DIR}/{system_id}.json"
    _github_save_file(filename, {system_id: system_data})


def _migrate_monolith_to_split():
    """One-time migration: if monolith cortex_members.json exists on GitHub, split it into per-system files."""
    mono_data, _ = _github_get_file(JSON_FILE)
    if not mono_data or not mono_data.get("systems"):
        return None

    systems = mono_data["systems"]
    print(f"[INFO] Migrating {len(systems)} systems from monolith {JSON_FILE} to per-system files...")
    for sys_id, sys_data in systems.items():
        _github_save_system(sys_id, sys_data)
    print(f"[INFO] Migration complete — {len(systems)} system files created in {SYSTEMS_DIR}/")
    return systems


# --- Load systems data ---
# Strategy:
# 1. Try loading per-system files from GitHub (systems/ directory)
# 2. If none found, try migrating from monolith cortex_members.json on GitHub
# 3. Fall back to local disk
# 4. Fall back to empty

systems_data = {"systems": {}}

if GITHUB_TOKEN:
    # Load per-system files
    split_systems = _github_load_all_systems()
    if split_systems:
        systems_data["systems"] = split_systems
        print(f"[INFO] Loaded {len(split_systems)} systems from GitHub per-system files")

    # Always load moderation/admin data from main monolith file
    mono_data, _ = _github_get_file(JSON_FILE)
    if mono_data:
        if mono_data.get("_moderation"):
            systems_data["_moderation"] = mono_data["_moderation"]
            print(f"[INFO] Loaded moderation state from {JSON_FILE}")
        
        # Also check monolith for any systems not yet in per-system files
        if mono_data.get("systems"):
            new_from_mono = 0
            for sys_id, sys_data in mono_data["systems"].items():
                if sys_id not in systems_data["systems"]:
                    systems_data["systems"][sys_id] = sys_data
                    _github_save_system(sys_id, sys_data)
                    new_from_mono += 1
            if new_from_mono:
                print(f"[INFO] Restored {new_from_mono} missing systems from monolith to per-system files")

    if not systems_data["systems"]:
        print("[INFO] No systems found on GitHub")

if os.path.exists(JSON_FILE):
    with open(JSON_FILE, "r") as f:
        local_data = json.load(f)
    # Always load moderation data from local file if it exists
    if local_data.get("_moderation"):
        systems_data.setdefault("_moderation", {}).update(local_data["_moderation"])
    # Load systems if no systems were found from GitHub
    if not systems_data["systems"] and local_data.get("systems"):
        systems_data["systems"] = local_data["systems"]
        print(f"[INFO] Loaded {JSON_FILE} from local disk ({len(systems_data['systems'])} systems)")
        # Push all to per-system files
        if GITHUB_TOKEN:
            for sys_id, sys_data in systems_data["systems"].items():
                _github_save_system(sys_id, sys_data)
            print(f"[INFO] Pushed {len(systems_data['systems'])} systems to per-system files")

if not systems_data["systems"]:
    print("[INFO] No existing data found, starting fresh")

# Write to local disk for the running session
with open(JSON_FILE, "w") as f:
    json.dump(systems_data, f, indent=4)


def save_systems():
    """Save all systems locally and queue per-system GitHub saves in background."""
    with open(JSON_FILE, "w") as f:
        json.dump(systems_data, f, indent=4)
    if GITHUB_TOKEN:
        _queue_github_save(systems_data)


def save_system(system_id):
    """Save locally and push just one system to GitHub in background."""
    with open(JSON_FILE, "w") as f:
        json.dump(systems_data, f, indent=4)
    if GITHUB_TOKEN:
        sys_data = systems_data.get("systems", {}).get(system_id)
        if sys_data:
            _queue_system_save(system_id, sys_data)

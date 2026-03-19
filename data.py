# data.py — Data persistence, GitHub helpers, and systems_data

import os
import json
import base64
import urllib.request
import urllib.error

from config import JSON_FILE, GITHUB_TOKEN, GITHUB_REPO


# --- GitHub persistence helpers ---
def _github_get_file(filename):
    """Get file content and sha from GitHub."""
    url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{filename}"
    req = urllib.request.Request(url, headers={
        "Authorization": f"token {GITHUB_TOKEN}",
        "Accept": "application/vnd.github.v3+json",
    })
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            content = base64.b64decode(data["content"]).decode("utf-8")
            return json.loads(content), data["sha"]
    except Exception:
        return None, None


def _github_save_file(filename, data_obj, retries=3):
    """Save JSON data to GitHub repo with retry logic and compact encoding."""
    if not GITHUB_TOKEN:
        return
    # Use separators to minimize JSON size (no extra whitespace)
    json_bytes = json.dumps(data_obj, separators=(",", ":")).encode("utf-8")
    encoded = base64.b64encode(json_bytes).decode("utf-8")
    size_mb = len(json_bytes) / (1024 * 1024)
    if size_mb > 90:
        print(f"[WARN] {filename} is {size_mb:.1f}MB — approaching GitHub 100MB limit")

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
            timeout = 30 + int(size_mb * 5)  # scale timeout with file size
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                resp.read()
            return  # success
        except Exception as e:
            print(f"[WARN] GitHub save attempt {attempt}/{retries} for {filename} failed: {e}")
            if attempt < retries:
                import time
                time.sleep(2 * attempt)  # backoff
    print(f"[ERROR] All {retries} attempts to push {filename} to GitHub failed")


# --- Load systems data ---
# Always prefer GitHub as the source of truth when a token is configured,
# because Railway's filesystem resets on every deploy and the committed
# JSON in the repo is stale compared to runtime changes saved to GitHub.
_gh_data = None
if GITHUB_TOKEN:
    _gh_data, _ = _github_get_file(JSON_FILE)

if _gh_data:
    systems_data = _gh_data
    # Write to local disk so the bot can read/write during this session
    with open(JSON_FILE, "w") as f:
        json.dump(systems_data, f, indent=4)
    print(f"[INFO] Loaded {JSON_FILE} from GitHub ({len(systems_data.get('systems', {}))} systems)")
elif os.path.exists(JSON_FILE):
    with open(JSON_FILE, "r") as f:
        systems_data = json.load(f)
    print(f"[INFO] Loaded {JSON_FILE} from local disk ({len(systems_data.get('systems', {}))} systems)")
else:
    systems_data = {"systems": {}}
    print(f"[INFO] No existing data found, starting fresh")


def save_systems():
    with open(JSON_FILE, "w") as f:
        json.dump(systems_data, f, indent=4)
    _github_save_file(JSON_FILE, systems_data)

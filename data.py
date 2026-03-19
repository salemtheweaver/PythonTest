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


def _github_save_file(filename, data_obj):
    """Save JSON data to GitHub repo."""
    if not GITHUB_TOKEN:
        return
    _, sha = _github_get_file(filename)
    content = base64.b64encode(json.dumps(data_obj, indent=4).encode("utf-8")).decode("utf-8")
    body = json.dumps({
        "message": f"Auto-update {filename}",
        "content": content,
        **({"sha": sha} if sha else {}),
    }).encode("utf-8")
    url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{filename}"
    req = urllib.request.Request(url, data=body, method="PUT", headers={
        "Authorization": f"token {GITHUB_TOKEN}",
        "Accept": "application/vnd.github.v3+json",
        "Content-Type": "application/json",
    })
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            resp.read()
    except Exception as e:
        print(f"[WARN] Failed to push {filename} to GitHub: {e}")


# --- Load systems data ---
if os.path.exists(JSON_FILE):
    with open(JSON_FILE, "r") as f:
        systems_data = json.load(f)
else:
    # Try loading from GitHub on fresh deploy
    _gh_data, _ = _github_get_file(JSON_FILE)
    if _gh_data:
        systems_data = _gh_data
        with open(JSON_FILE, "w") as f:
            json.dump(systems_data, f, indent=4)
    else:
        systems_data = {"systems": {}}


def save_systems():
    with open(JSON_FILE, "w") as f:
        json.dump(systems_data, f, indent=4)
    _github_save_file(JSON_FILE, systems_data)



# Required imports
import os
import json
import random
import asyncio
import base64
from copy import deepcopy
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
import urllib.request
import urllib.error
import discord
from discord import app_commands
from discord.ext import commands, tasks
from dotenv import load_dotenv

# Load Discord bot token securely from environment variable
load_dotenv()
TOKEN = os.getenv("DISCORD_BOT_TOKEN")
if not TOKEN:
    raise RuntimeError("DISCORD_BOT_TOKEN environment variable not set.")
ADMIN_USER_ID = os.getenv("CORTEX_ADMIN_USER_ID")
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
GITHUB_REPO = os.getenv("GITHUB_REPO", "salemtheweaver/PythonTest")

JSON_FILE = "cortex_members.json"
TAGS_FILE = "tags.json"

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

intents = discord.Intents.default()
intents.message_content = True

class CortexCommandTree(discord.app_commands.CommandTree):
    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        return await _global_interaction_check(interaction)

bot = commands.Bot(
    command_prefix=commands.when_mentioned_or("Cor;", "cor;"),
    intents=intents,
    help_command=None,
    tree_cls=CortexCommandTree,
)
tree = bot.tree

PROXY_PREFIX = ";;"
PROXY_WEBHOOK_NAME = "Cortex Proxy"
EXTERNAL_MSG_LIMIT_COUNT = 5
EXTERNAL_MSG_LIMIT_SECONDS = 60
external_msg_rate_state = {}
EXTERNAL_TARGET_LIMIT_SECONDS = 20
external_target_rate_state = {}
EXTERNAL_AUDIT_MAX = 200
PENDING_TIMEZONE_PROMPTS = {}
SCHEDULED_MESSAGES = {}  # {user_id: [{"send_at": datetime, "message": str}, ...]}

MOD_COMMANDS = {
    "modreports",
    "modwarn",
    "modsuspend",
    "modban",
    "modunban",
}

ALLOWED_WHEN_BOT_BANNED = {"modappeal"}

SINGLET_ALLOWED_COMMANDS = {
    "register",
    "allowexternal",
    "externalprivacy",
    "externalstatus",
    "sendexternal",
    "blockuser",
    "unblockuser",
    "blockedusers",
    "externaltrustedonly",
    "trustuser",
    "untrustuser",
    "trustedusers",
    "muteuser",
    "unmuteuser",
    "mutedusers",
    "tempblockuser",
    "tempblockedusers",
    "externalpending",
    "approveexternal",
    "recentexternal",
    "externallimits",
    "externalquiethours",
    "externalretention",
    "checkin",
    "checkinstatus",
    "weeklymoodsummary",
    "settimezone",
    "timezonestatus",
    "refresh",
    "synccommands",
    "reportexternal",
    "setmode",
    "currentmode",
    "modestats",
    "systemprivacy",
    "alterprivacy",
    "privacystatus",
    "sendmessage",
}

PROFILE_PRIVACY_LEVELS = {"private", "trusted", "public"}

TIMEZONE_ALIASES = {
    "EST": "America/New_York",
    "EDT": "America/New_York",
    "CST": "America/Chicago",
    "CDT": "America/Chicago",
    "MST": "America/Denver",
    "MDT": "America/Denver",
    "PST": "America/Los_Angeles",
    "PDT": "America/Los_Angeles",
    "AKST": "America/Anchorage",
    "AKDT": "America/Anchorage",
    "HST": "Pacific/Honolulu",
    "GMT": "Etc/GMT",
    "BST": "Europe/London",
    "CET": "Europe/Paris",
    "CEST": "Europe/Paris",
    "EET": "Europe/Athens",
    "JST": "Asia/Tokyo",
    "KST": "Asia/Seoul",
    "IST": "Asia/Kolkata",
    "AEST": "Australia/Sydney",
    "AEDT": "Australia/Sydney",
    "UTC": "UTC",
}

COMMON_TIMEZONES = [
    "UTC",
    "America/New_York",
    "America/Chicago",
    "America/Denver",
    "America/Los_Angeles",
    "America/Anchorage",
    "Pacific/Honolulu",
    "Europe/London",
    "Europe/Paris",
    "Europe/Berlin",
    "Europe/Athens",
    "Asia/Tokyo",
    "Asia/Seoul",
    "Asia/Kolkata",
    "Asia/Singapore",
    "Australia/Sydney",
    "Australia/Perth",
]

TIMEZONE_FIXED_OFFSETS = {
    "UTC": 0,
    "Etc/GMT": 0,
    "America/New_York": -5,
    "America/Chicago": -6,
    "America/Denver": -7,
    "America/Los_Angeles": -8,
    "America/Anchorage": -9,
    "Pacific/Honolulu": -10,
    "Europe/London": 0,
    "Europe/Paris": 1,
    "Europe/Berlin": 1,
    "Europe/Athens": 2,
    "Asia/Tokyo": 9,
    "Asia/Seoul": 9,
    "Asia/Kolkata": 5.5,
    "Asia/Singapore": 8,
    "Australia/Sydney": 10,
    "Australia/Perth": 8,
}

# Helper to save systems
def save_systems():
    with open(JSON_FILE, "w") as f:
        json.dump(systems_data, f, indent=4)
    _github_save_file(JSON_FILE, systems_data)

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

def get_moderation_state():
    state = systems_data.setdefault("_moderation", {})
    state.setdefault("reports", [])
    state.setdefault("sanctions", {})
    state.setdefault("events", [])
    return state

def get_user_sanctions(user_id):
    state = get_moderation_state()
    sanctions = state.setdefault("sanctions", {}).setdefault(str(user_id), {})
    sanctions.setdefault("warnings", 0)
    sanctions.setdefault("external_banned", False)
    sanctions.setdefault("bot_banned", False)
    sanctions.setdefault("suspended_until", None)
    sanctions.setdefault("scope", "external")
    sanctions.setdefault("reason", None)
    return sanctions

def is_user_suspended(user_id, scope="external"):
    sanctions = get_user_sanctions(user_id)
    until_iso = sanctions.get("suspended_until")
    if not until_iso:
        return False
    try:
        until_dt = datetime.fromisoformat(until_iso)
    except ValueError:
        sanctions["suspended_until"] = None
        return False
    if datetime.now(timezone.utc) >= until_dt:
        sanctions["suspended_until"] = None
        return False
    sanction_scope = sanctions.get("scope", "external")
    return sanction_scope == "all" or scope == "external"

def is_bot_moderator_user(user_id):
    if ADMIN_USER_ID and str(user_id) == str(ADMIN_USER_ID):
        return True
    if bot.application and bot.application.owner:
        owner = bot.application.owner
        if hasattr(owner, "id") and str(user_id) == str(owner.id):
            return True
    return False

async def ensure_moderator(interaction: discord.Interaction):
    if is_bot_moderator_user(interaction.user.id):
        return True
    if not interaction.response.is_done():
        await interaction.response.send_message("You do not have permission to use moderation commands.", ephemeral=True)
    return False

def add_mod_event(action, target_user_id=None, moderator_user_id=None, details=None):
    state = get_moderation_state()
    events = state.setdefault("events", [])
    events.append({
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "action": action,
        "target_user_id": str(target_user_id) if target_user_id is not None else None,
        "moderator_user_id": str(moderator_user_id) if moderator_user_id is not None else None,
        "details": details,
    })
    if len(events) > 300:
        state["events"] = events[-300:]

# Helper to get system for a user (by Discord user id)
def get_user_system_id(user_id):
    for sys_id, sys in systems_data["systems"].items():
        if str(sys.get("owner_id")) == str(user_id):
            return sys_id
    return None

def get_user_mode(user_id):
    system_id = get_user_system_id(user_id)
    if not system_id:
        return None
    system = systems_data.get("systems", {}).get(system_id, {})
    return system.get("mode", "system")


def parse_bool_token(value):
    if value is None:
        return None
    cleaned = str(value).strip().lower()
    if cleaned in {"true", "t", "yes", "y", "1", "on", "enable", "enabled"}:
        return True
    if cleaned in {"false", "f", "no", "n", "0", "off", "disable", "disabled"}:
        return False
    return None


@bot.check
async def prefix_command_gate(ctx: commands.Context):
    if ctx.command is None:
        return True

    command_name = ctx.command.name

    if command_name in MOD_COMMANDS and not is_bot_moderator_user(ctx.author.id):
        await ctx.send("You do not have permission to use moderation commands.")
        return False

    sanctions = get_user_sanctions(ctx.author.id)
    if sanctions.get("bot_banned") and command_name not in ALLOWED_WHEN_BOT_BANNED:
        await ctx.send("You are currently restricted from using this bot. You may use /modappeal.")
        return False

    if is_user_suspended(ctx.author.id, scope="all") and command_name not in ALLOWED_WHEN_BOT_BANNED:
        await ctx.send("You are temporarily suspended from bot usage.")
        return False

    if command_name == "register":
        return True

    mode = get_user_mode(ctx.author.id)
    if mode != "singlet":
        return True

    if command_name in SINGLET_ALLOWED_COMMANDS:
        return True

    await ctx.send(
        "This command is unavailable in singlet mode. Use external messaging, wellness, focus mode, and timezone commands."
    )
    return False

async def _global_interaction_check(interaction: discord.Interaction) -> bool:
    command = interaction.command
    if command is None:
        return True

    command_name = command.name

    if command_name in MOD_COMMANDS:
        if not is_bot_moderator_user(interaction.user.id):
            if not interaction.response.is_done():
                await interaction.response.send_message("You do not have permission to use moderation commands.", ephemeral=True)
            return False
        return True

    sanctions = get_user_sanctions(interaction.user.id)
    if sanctions.get("bot_banned") and command_name not in ALLOWED_WHEN_BOT_BANNED:
        if not interaction.response.is_done():
            await interaction.response.send_message(
                "You are currently restricted from using this bot. You may use /modappeal.",
                ephemeral=True,
            )
        return False

    if is_user_suspended(interaction.user.id, scope="all") and command_name not in ALLOWED_WHEN_BOT_BANNED:
        if not interaction.response.is_done():
            await interaction.response.send_message(
                "You are temporarily suspended from bot usage.",
                ephemeral=True,
            )
        return False

    if command_name == "register":
        return True

    mode = get_user_mode(interaction.user.id)
    if mode != "singlet":
        return True

    if command_name in SINGLET_ALLOWED_COMMANDS:
        return True

    if not interaction.response.is_done():
        await interaction.response.send_message(
            "This command is unavailable in singlet mode. Use external messaging, wellness, focus mode, and timezone commands.",
            ephemeral=True,
        )
    return False

# Helper to get members dict for a system or subsystem
def get_system_members(system_id, subsystem_id=None):
    if system_id not in systems_data["systems"]:
        return None
    
    system = systems_data["systems"][system_id]
    
    if subsystem_id is None:
        # Return main system members
        return system.get("members", {})
    else:
        # Return subsystem members
        subsystems = system.get("subsystems", {})
        if subsystem_id not in subsystems:
            return None
        return subsystems[subsystem_id].get("members", {})

def get_scope_label(subsystem_id):
    return f"subsystem `{subsystem_id}`" if subsystem_id else "main system"

def iter_system_member_dicts(system):
    """Yield (scope_id, members_dict) for main system and each subsystem."""
    yield (None, system.get("members", {}))
    for sub_id, sub_data in system.get("subsystems", {}).items():
        yield (sub_id, sub_data.get("members", {}))


def get_member_sort_mode(system):
    """Return member list sort mode for a system."""
    settings = system.setdefault("member_display", {})
    mode = settings.get("sort_mode", "id")
    if mode not in {"id", "alphabetical"}:
        mode = "id"
        settings["sort_mode"] = mode
    return mode


def sort_member_rows(member_rows, sort_mode):
    """Sort (scope_id, member_id, member) rows by configured mode."""
    if sort_mode == "alphabetical":
        return sorted(
            member_rows,
            key=lambda row: (
                str(row[2].get("display_name") or row[2].get("name") or "").lower(),
                str(row[1]).lower(),
            ),
        )

    def id_sort_key(row):
        member_id = str(row[1])
        try:
            return (0, int(member_id))
        except ValueError:
            return (1, member_id.lower())

    return sorted(member_rows, key=id_sort_key)


def get_member_lookup_keys(member):
    """Build normalized identity keys used for member matching."""
    keys = set()
    for field in ("name", "display_name"):
        raw = (member.get(field) or "").strip().lower()
        if raw:
            keys.add(raw)
    return keys


def resolve_member_identifier(members, token):
    """Resolve a member by ID or name/display name.

    Returns (member_id, member_dict, error_message).
    """
    if not isinstance(members, dict):
        return None, None, "Member scope not found."

    raw = str(token or "").strip()
    if not raw:
        return None, None, "Provide a member ID or name."

    if raw in members:
        return raw, members[raw], None

    needle = raw.lower()

    def member_names(member):
        vals = []
        for field in ("name", "display_name"):
            text = str(member.get(field) or "").strip()
            if text:
                vals.append(text)
        return vals

    def dedupe(matches):
        seen = set()
        out = []
        for mid, m in matches:
            if mid in seen:
                continue
            seen.add(mid)
            out.append((mid, m))
        return out

    exact = []
    prefix = []
    contains = []
    for mid, member in members.items():
        names = member_names(member)
        lowered = [n.lower() for n in names]
        if any(n == needle for n in lowered):
            exact.append((mid, member))
            continue
        if any(n.startswith(needle) for n in lowered):
            prefix.append((mid, member))
            continue
        if any(needle in n for n in lowered):
            contains.append((mid, member))

    for candidates in (dedupe(exact), dedupe(prefix), dedupe(contains)):
        if len(candidates) == 1:
            mid, member = candidates[0]
            return mid, member, None
        if len(candidates) > 1:
            labels = [f"{m.get('name', mid)} (`{mid}`)" for mid, m in candidates[:6]]
            suffix = " ..." if len(candidates) > 6 else ""
            return None, None, "Ambiguous member name. Matches: " + ", ".join(labels) + suffix + "\nHint: use full name or member ID."

    return None, None, "Member not found. Hint: use full name or member ID."


def resolve_member_identifier_in_system(system, token, subsystem_id=None):
    """Resolve a member by ID/name across one scope or the entire system.

    Returns (scope_id, members_dict, member_id, member_dict, error_message).
    """
    raw = str(token or "").strip()
    if not raw:
        return None, None, None, None, "Provide a member ID or name."

    if subsystem_id is not None:
        members = system.get("subsystems", {}).get(subsystem_id, {}).get("members")
        if members is None:
            return None, None, None, None, f"Member not found in {get_scope_label(subsystem_id)}."
        resolved_id, resolved_member, err = resolve_member_identifier(members, raw)
        if err:
            return None, None, None, None, err
        return subsystem_id, members, resolved_id, resolved_member, None

    candidates = []
    needle = raw.lower()

    for scope_id, members in iter_system_member_dicts(system):
        for mid, member in members.items():
            names = []
            for field in ("name", "display_name"):
                text = str(member.get(field) or "").strip()
                if text:
                    names.append(text)

            lowered = [n.lower() for n in names]
            rank = None
            if raw == mid:
                rank = 0
            elif any(n == needle for n in lowered):
                rank = 1
            elif any(n.startswith(needle) for n in lowered):
                rank = 2
            elif any(needle in n for n in lowered):
                rank = 3

            if rank is not None:
                candidates.append((rank, scope_id, members, mid, member))

    if not candidates:
        return None, None, None, None, "Member not found. Hint: use full name or member ID."

    best_rank = min(c[0] for c in candidates)
    best = [c for c in candidates if c[0] == best_rank]

    # Deduplicate by (scope, member_id) in case name/display_name both matched.
    deduped = []
    seen = set()
    for item in best:
        key = (item[1], item[3])
        if key in seen:
            continue
        seen.add(key)
        deduped.append(item)

    if len(deduped) == 1:
        _, scope_id, members, mid, member = deduped[0]
        return scope_id, members, mid, member, None

    labels = [f"{item[4].get('name', item[3])} (`{item[3]}` in {get_scope_label(item[1])})" for item in deduped[:6]]
    suffix = " ..." if len(deduped) > 6 else ""
    return None, None, None, None, "Ambiguous member name. Matches: " + ", ".join(labels) + suffix + "\nHint: use full name or member ID."


def get_group_settings(system):
    """Return and initialize group settings for a system."""
    settings = system.setdefault("group_settings", {})
    settings.setdefault("groups", {})
    settings.setdefault("order", [])

    groups = settings["groups"]
    # Normalize group entries and keep IDs as strings.
    normalized = {}
    for group_id, group in groups.items():
        gid = str(group_id)
        group_obj = group if isinstance(group, dict) else {}
        parent_id = group_obj.get("parent_id")
        normalized[gid] = {
            "id": gid,
            "name": str(group_obj.get("name") or f"Group {gid}"),
            "parent_id": str(parent_id) if parent_id is not None else None,
        }
    settings["groups"] = normalized

    valid_ids = set(normalized.keys())
    order = [str(gid) for gid in settings.get("order", []) if str(gid) in valid_ids]
    for gid in sorted(valid_ids):
        if gid not in order:
            order.append(gid)
    settings["order"] = order
    return settings


def get_next_group_id(system):
    """Return the next group ID (G001, G002, ...)."""
    settings = get_group_settings(system)
    max_num = 0
    for group_id in settings.get("groups", {}).keys():
        gid = str(group_id)
        if gid.startswith("G"):
            try:
                max_num = max(max_num, int(gid[1:]))
            except ValueError:
                continue
    return f"G{max_num + 1:03d}"


def get_group_path_text(groups, group_id, max_depth=12):
    """Return nested path text like Parent > Child > Group."""
    current_id = str(group_id)
    parts = []
    seen = set()
    depth = 0
    while current_id and current_id in groups and current_id not in seen and depth < max_depth:
        seen.add(current_id)
        group = groups[current_id]
        parts.append(str(group.get("name") or current_id))
        parent_id = group.get("parent_id")
        current_id = str(parent_id) if parent_id is not None else None
        depth += 1
    parts.reverse()
    return " > ".join(parts) if parts else str(group_id)


def sorted_group_ids_for_member(system, member):
    """Return member group IDs sorted by configured system order and nested path."""
    settings = get_group_settings(system)
    groups = settings.get("groups", {})
    order_index = {gid: i for i, gid in enumerate(settings.get("order", []))}

    raw_ids = member.get("groups", []) or []
    group_ids = []
    seen = set()
    for gid in raw_ids:
        gid_str = str(gid)
        if gid_str in groups and gid_str not in seen:
            group_ids.append(gid_str)
            seen.add(gid_str)

    group_ids.sort(
        key=lambda gid: (
            order_index.get(gid, 10**9),
            get_group_path_text(groups, gid).lower(),
            gid,
        )
    )
    return group_ids


def format_member_group_lines(system, member):
    """Format member groups with nested paths and IDs for card display."""
    settings = get_group_settings(system)
    groups = settings.get("groups", {})
    ids = sorted_group_ids_for_member(system, member)
    if not ids:
        return "None"
    return "\n".join([f"- {get_group_path_text(groups, gid)} (`{gid}`)" for gid in ids])


def build_group_order_embed(system, order, focus_index):
    """Build an embed previewing current group card order and focused item."""
    settings = get_group_settings(system)
    groups = settings.get("groups", {})

    lines = []
    for idx, gid in enumerate(order):
        marker = "->" if idx == focus_index else "  "
        path = get_group_path_text(groups, gid)
        lines.append(f"{marker} {idx + 1}. `{gid}` - {path}")

    description = "\n".join(lines) if lines else "No groups found."
    embed = discord.Embed(
        title="Group Order Editor",
        description=description,
        color=discord.Color.dark_teal(),
    )
    embed.add_field(
        name="How to use",
        value="Use Focus Up/Down to select a group, Move Up/Down to reorder, then Save.",
        inline=False,
    )
    if order:
        focus_gid = order[focus_index]
        embed.set_footer(text=f"Focused: {focus_gid} ({focus_index + 1}/{len(order)})")
    return embed


class GroupOrderView(discord.ui.View):
    def __init__(self, owner_id, system):
        super().__init__(timeout=180)
        self.owner_id = owner_id
        self.system = system
        settings = get_group_settings(system)
        groups = settings.get("groups", {})
        self.order = [gid for gid in settings.get("order", []) if gid in groups]
        self.focus_index = 0
        self._update_button_state()

    def _update_button_state(self):
        no_groups = len(self.order) == 0
        at_top = self.focus_index <= 0
        at_bottom = self.focus_index >= len(self.order) - 1

        self.focus_up_button.disabled = no_groups or at_top
        self.focus_down_button.disabled = no_groups or at_bottom
        self.move_up_button.disabled = no_groups or at_top
        self.move_down_button.disabled = no_groups or at_bottom
        self.save_button.disabled = no_groups

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.owner_id:
            await interaction.response.send_message("Only the command author can use this UI.", ephemeral=True)
            return False
        return True

    def current_embed(self):
        return build_group_order_embed(self.system, self.order, self.focus_index)

    async def on_timeout(self):
        self.focus_up_button.disabled = True
        self.focus_down_button.disabled = True
        self.move_up_button.disabled = True
        self.move_down_button.disabled = True
        self.save_button.disabled = True
        self.cancel_button.disabled = True

    @discord.ui.button(label="Focus Up", style=discord.ButtonStyle.secondary)
    async def focus_up_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self.focus_index > 0:
            self.focus_index -= 1
        self._update_button_state()
        await interaction.response.edit_message(embed=self.current_embed(), view=self)

    @discord.ui.button(label="Focus Down", style=discord.ButtonStyle.secondary)
    async def focus_down_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self.focus_index < len(self.order) - 1:
            self.focus_index += 1
        self._update_button_state()
        await interaction.response.edit_message(embed=self.current_embed(), view=self)

    @discord.ui.button(label="Move Up", style=discord.ButtonStyle.primary)
    async def move_up_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self.focus_index > 0:
            i = self.focus_index
            self.order[i - 1], self.order[i] = self.order[i], self.order[i - 1]
            self.focus_index -= 1
        self._update_button_state()
        await interaction.response.edit_message(embed=self.current_embed(), view=self)

    @discord.ui.button(label="Move Down", style=discord.ButtonStyle.primary)
    async def move_down_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self.focus_index < len(self.order) - 1:
            i = self.focus_index
            self.order[i + 1], self.order[i] = self.order[i], self.order[i + 1]
            self.focus_index += 1
        self._update_button_state()
        await interaction.response.edit_message(embed=self.current_embed(), view=self)

    @discord.ui.button(label="Save", style=discord.ButtonStyle.success)
    async def save_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        settings = get_group_settings(self.system)
        groups = settings.get("groups", {})

        # Keep any newly created groups not shown in this session appended at the end.
        seen = set(self.order)
        merged_order = [gid for gid in self.order if gid in groups]
        for gid in settings.get("order", []):
            if gid in groups and gid not in seen:
                merged_order.append(gid)
        for gid in groups.keys():
            if gid not in seen and gid not in merged_order:
                merged_order.append(gid)

        settings["order"] = merged_order
        save_systems()
        await interaction.response.edit_message(content="Group order saved.", embed=None, view=None)
        self.stop()

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.danger)
    async def cancel_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.edit_message(content="Group order edit cancelled.", embed=None, view=None)
        self.stop()

def get_next_system_member_id(system_id):
    """Return next system-wide member ID as 5-digit zero-padded string."""
    system = systems_data.get("systems", {}).get(system_id)
    if not system:
        return "00001"

    max_numeric_id = 0
    for _, members_dict in iter_system_member_dicts(system):
        for member_id in members_dict.keys():
            try:
                numeric = int(str(member_id))
                if numeric > max_numeric_id:
                    max_numeric_id = numeric
            except ValueError:
                continue

    return f"{max_numeric_id + 1:05d}"

def get_migrations_state():
    """Return root migration state object."""
    return systems_data.setdefault("_migrations", {})

def migrate_system_member_ids(system):
    """Renumber all member IDs in one system and update dependent references.

    IDs become 5-digit zero-padded, sequential across main + all subsystems.
    Returns number of migrated members.
    """
    collected = []
    for scope_id, members_dict in iter_system_member_dicts(system):
        for old_id, member in members_dict.items():
            old_id_str = str(old_id)
            try:
                numeric_old = int(old_id_str)
            except ValueError:
                numeric_old = 10**9
            collected.append((numeric_old, member.get("created_at", ""), str(scope_id), old_id_str, scope_id, member))

    if not collected:
        return 0

    collected.sort(key=lambda item: (item[0], item[1], item[2], item[3]))

    id_map = {}
    next_num = 1
    for _, _, _, old_id_str, scope_id, _ in collected:
        id_map[(scope_id, old_id_str)] = f"{next_num:05d}"
        next_num += 1

    for scope_id, members_dict in list(iter_system_member_dicts(system)):
        rebuilt = {}
        for old_id, member in members_dict.items():
            old_id_str = str(old_id)
            new_id = id_map[(scope_id, old_id_str)]
            member["id"] = new_id

            current = member.get("current_front")
            if current and isinstance(current.get("cofronts"), list):
                current["cofronts"] = [
                    id_map.get((scope_id, str(co_id)), str(co_id))
                    for co_id in current.get("cofronts", [])
                ]

            for entry in member.get("front_history", []):
                if isinstance(entry, dict) and isinstance(entry.get("cofronts"), list):
                    entry["cofronts"] = [
                        id_map.get((scope_id, str(co_id)), str(co_id))
                        for co_id in entry.get("cofronts", [])
                    ]

            rebuilt[new_id] = member

        if scope_id is None:
            system["members"] = rebuilt
        else:
            system.setdefault("subsystems", {}).setdefault(scope_id, {}).setdefault("members", {})
            system["subsystems"][scope_id]["members"] = rebuilt

    autoproxy = get_autoproxy_settings(system)
    latch_scope_id = autoproxy.get("latch_scope_id")
    latch_member_id = autoproxy.get("latch_member_id")
    if latch_member_id is not None:
        autoproxy["latch_member_id"] = id_map.get((latch_scope_id, str(latch_member_id)), latch_member_id)

    for server_settings in system.get("autoproxy_server_overrides", {}).values():
        if not isinstance(server_settings, dict):
            continue
        server_latch_scope_id = server_settings.get("latch_scope_id")
        server_latch_member_id = server_settings.get("latch_member_id")
        if server_latch_member_id is not None:
            server_settings["latch_member_id"] = id_map.get(
                (server_latch_scope_id, str(server_latch_member_id)),
                server_latch_member_id,
            )

    member_overrides = system.get("server_member_overrides", {})
    if isinstance(member_overrides, dict):
        for guild_key, by_member in list(member_overrides.items()):
            if not isinstance(by_member, dict):
                continue
            rebuilt_member_overrides = {}
            for old_member_id, override_entry in by_member.items():
                new_member_id = None
                for (scope_id, old_id), mapped_id in id_map.items():
                    if str(old_id) == str(old_member_id):
                        new_member_id = mapped_id
                        break
                if new_member_id is None:
                    new_member_id = str(old_member_id)
                rebuilt_member_overrides[str(new_member_id)] = override_entry
            member_overrides[guild_key] = rebuilt_member_overrides

    return len(collected)

def get_fronting_member_for_user(user_id):
    """Return (scope_id, member_dict) for the most recent current front in user's system."""
    system_id = get_user_system_id(user_id)
    if not system_id:
        return None, None

    system = systems_data.get("systems", {}).get(system_id)
    if not system:
        return None, None

    candidates = []
    for scope_id, members_dict in iter_system_member_dicts(system):
        for member in members_dict.values():
            current = member.get("current_front")
            if not current:
                continue
            start_iso = current.get("start")
            if start_iso:
                candidates.append((start_iso, scope_id, member))

    if not candidates:
        return None, None

    # Pick the most recently started front session.
    candidates.sort(key=lambda item: item[0], reverse=True)
    _, scope_id, member = candidates[0]
    return scope_id, member

def get_autoproxy_settings(system):
    """Return and initialize autoproxy settings for a system."""
    settings = system.setdefault("autoproxy", {})
    settings.setdefault("mode", "off")
    settings.setdefault("latch_scope_id", None)
    settings.setdefault("latch_member_id", None)
    return settings

def get_server_autoproxy_settings(system, guild_id, create=False):
    """Return and initialize autoproxy override settings for a guild."""
    if guild_id is None:
        return None
    overrides = system.setdefault("autoproxy_server_overrides", {})
    key = str(guild_id)
    if key not in overrides:
        if not create:
            return None
        overrides[key] = {
            "mode": "off",
            "latch_scope_id": None,
            "latch_member_id": None,
        }
    settings = overrides[key]
    settings.setdefault("mode", "off")
    settings.setdefault("latch_scope_id", None)
    settings.setdefault("latch_member_id", None)
    return settings

def get_effective_autoproxy_settings(system, guild_id):
    """Return active autoproxy settings and scope label for a message context."""
    if guild_id is not None:
        server_settings = get_server_autoproxy_settings(system, guild_id, create=False)
        if server_settings is not None:
            return server_settings, "server"
    return get_autoproxy_settings(system), "global"

def apply_autoproxy_mode(settings, mode):
    """Apply autoproxy mode and clear latch state when turning off."""
    settings["mode"] = mode
    if mode == "off":
        settings["latch_scope_id"] = None
        settings["latch_member_id"] = None

def get_system_proxy_tag(system):
    """Return system-level tag suffix for proxied messages."""
    return system.get("system_tag")

def get_system_profile(system):
    """Return and initialize system profile card data."""
    profile = system.setdefault("system_profile", {})
    profile.setdefault("collective_pronouns", None)
    profile.setdefault("description", None)
    profile.setdefault("profile_pic", None)
    profile.setdefault("banner", None)
    profile.setdefault("color", "00DE9B")
    return profile

def get_server_appearance(system, guild_id):
    """Return server-specific appearance override dict for a guild, or None if none is set."""
    if not guild_id:
        return None
    entry = system.get("server_appearance_overrides", {}).get(str(guild_id))
    return entry if entry else None

def get_server_member_appearance(system, guild_id, member_id):
    """Return server-specific member appearance override for a guild/member, or None if none is set."""
    if not guild_id or member_id is None:
        return None
    by_guild = system.get("server_member_overrides", {}).get(str(guild_id), {})
    entry = by_guild.get(str(member_id))
    return entry if entry else None

def resolve_member_for_override(system, member_id, subsystem_id=None):
    """Resolve member in a system for override edits.

    Returns (scope_id, member_dict, error_key)
    error_key is one of: None, "subsystem_not_found", "not_found", "ambiguous".
    """
    target_id = str(member_id)

    if subsystem_id is not None:
        subsystems = system.get("subsystems", {})
        if subsystem_id not in subsystems:
            return None, None, "subsystem_not_found"
        member = subsystems[subsystem_id].get("members", {}).get(target_id)
        if not member:
            return None, None, "not_found"
        return subsystem_id, member, None

    matches = []
    for scope_id, members_dict in iter_system_member_dicts(system):
        member = members_dict.get(target_id)
        if member:
            matches.append((scope_id, member))

    if not matches:
        return None, None, "not_found"
    if len(matches) > 1:
        return None, None, "ambiguous"
    return matches[0][0], matches[0][1], None

def get_front_reminder_settings(system):
    """Return and initialize DM front reminder settings for a system."""
    settings = system.setdefault("front_reminders", {})
    settings.setdefault("enabled", False)
    settings.setdefault("hours", 4)
    return settings

def get_system_timezone_name(system):
    """Return system timezone name, defaulting to UTC."""
    tz_name = system.get("timezone", "UTC")
    return tz_name if isinstance(tz_name, str) and tz_name else "UTC"

def normalize_timezone_name(raw_name):
    """Normalize user timezone input by applying common aliases."""
    if raw_name is None:
        return None
    cleaned = str(raw_name).strip()
    if not cleaned:
        return None
    alias = TIMEZONE_ALIASES.get(cleaned.upper())
    return alias if alias else cleaned

def get_system_timezone(system):
    """Return tzinfo for system timezone, falling back to UTC."""
    tz_name = get_system_timezone_name(system)
    try:
        return ZoneInfo(tz_name)
    except ZoneInfoNotFoundError:
        offset_hours = TIMEZONE_FIXED_OFFSETS.get(tz_name)
        if offset_hours is not None:
            offset_seconds = int(offset_hours * 3600)
            return timezone(timedelta(seconds=offset_seconds))
        return timezone.utc

async def timezone_autocomplete(interaction: discord.Interaction, current: str):
    current_lower = (current or "").lower()
    options = []
    seen = set()

    for tz_name in COMMON_TIMEZONES:
        if current_lower in tz_name.lower() and tz_name not in seen:
            options.append(app_commands.Choice(name=tz_name, value=tz_name))
            seen.add(tz_name)
        if len(options) >= 25:
            return options

    for alias, tz_name in TIMEZONE_ALIASES.items():
        label = f"{alias} -> {tz_name}"
        if (current_lower in alias.lower() or current_lower in tz_name.lower()) and label not in seen:
            options.append(app_commands.Choice(name=label[:100], value=tz_name))
            seen.add(label)
        if len(options) >= 25:
            return options

    return options

def get_checkin_settings(system):
    """Return and initialize system check-in settings/state."""
    settings = system.setdefault("checkins", {})
    settings.setdefault("entries", [])
    settings.setdefault("weekly_dm_enabled", True)
    settings.setdefault("last_weekly_summary_week", None)
    return settings

def _recent_checkins(system, days=7):
    settings = get_checkin_settings(system)
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(days=days)
    results = []
    for entry in settings.get("entries", []):
        ts = entry.get("timestamp")
        if not ts:
            continue
        try:
            dt = datetime.fromisoformat(ts)
        except ValueError:
            continue
        if dt >= cutoff:
            results.append(entry)
    return results

def get_checkin_trend_text(system):
    """Build a simple trend message for recent system mood check-ins."""
    recent = _recent_checkins(system, days=7)
    if not recent:
        return "No check-ins yet this week."

    ratings = [int(e.get("rating", 0)) for e in recent if e.get("rating") is not None]
    if not ratings:
        return "No mood ratings available this week."

    avg_rating = sum(ratings) / len(ratings)
    if avg_rating <= 4:
        level = "low"
    elif avg_rating <= 7:
        level = "moderate"
    else:
        level = "high"

    direction = "stable"
    if len(ratings) >= 4:
        midpoint = len(ratings) // 2
        first_avg = sum(ratings[:midpoint]) / len(ratings[:midpoint])
        second_avg = sum(ratings[midpoint:]) / len(ratings[midpoint:])
        delta = second_avg - first_avg
        if delta >= 0.75:
            direction = "up"
        elif delta <= -0.75:
            direction = "down"

    return f"System mood trending {level} this week (avg {avg_rating:.1f}/10, {direction})."

def _rating_to_mood_emoji(rating):
    if rating <= 2:
        return "🟥"
    if rating <= 4:
        return "🟧"
    if rating <= 6:
        return "🟨"
    if rating <= 8:
        return "🟩"
    return "🟦"

def _build_checkin_sparkline(checkins, max_points=14):
    points = []
    for entry in checkins:
        ts = entry.get("timestamp")
        rating = entry.get("rating")
        if ts is None or rating is None:
            continue
        try:
            dt = datetime.fromisoformat(ts)
            numeric = int(rating)
        except (ValueError, TypeError):
            continue
        points.append((dt, numeric))

    if not points:
        return "No chart data yet."

    points.sort(key=lambda p: p[0])
    points = points[-max_points:]
    chart = "".join(_rating_to_mood_emoji(r) for _, r in points)
    labels = "".join(str(max(1, min(10, r))) for _, r in points)
    return f"{chart}\n{labels}"

def build_weekly_checkin_summary(system):
    """Build weekly check-in summary text for DM delivery."""
    recent = _recent_checkins(system, days=7)
    if not recent:
        return None

    ratings = [int(e.get("rating", 0)) for e in recent if e.get("rating") is not None]
    if not ratings:
        return None

    energy_counts = {}
    for entry in recent:
        energy = entry.get("energy", "unknown")
        energy_counts[energy] = energy_counts.get(energy, 0) + 1

    top_energy = max(energy_counts, key=energy_counts.get)
    avg_rating = sum(ratings) / len(ratings)
    trend_text = get_checkin_trend_text(system)
    sparkline = _build_checkin_sparkline(recent)

    return (
        "Weekly check-in summary:\n"
        f"- Mood chart (oldest to newest):\n{sparkline}\n"
        f"- Check-ins logged: {len(recent)}\n"
        f"- Average mood rating: {avg_rating:.1f}/10\n"
        f"- Lowest/Highest: {min(ratings)}/10 - {max(ratings)}/10\n"
        f"- Most common energy: {str(top_energy).replace('_', ' ').title()}\n"
        f"- Trend: {trend_text}"
    )

def current_week_key(now=None):
    iso = now.isocalendar()
    return f"{iso.year}-W{iso.week:02d}"

# -----------------------------
# Focus Mode helpers (singlet)
# -----------------------------

DEFAULT_FOCUS_MODES = [
    "studying", "gaming", "social", "burnout", "rest",
    "creative", "work", "exercise", "errands",
]

def get_focus_modes(system):
    """Return (and lazily initialise) the focus_modes dict on a system."""
    return system.setdefault("focus_modes", {"current": None, "history": [], "custom_modes": []})

def get_all_mode_names(system):
    """Returns the full list of valid mode names (defaults + custom) for a system."""
    fm = get_focus_modes(system)
    custom = [m.lower() for m in fm.get("custom_modes", [])]
    return DEFAULT_FOCUS_MODES + [m for m in custom if m not in DEFAULT_FOCUS_MODES]

def start_focus_mode(system, mode_name: str):
    """Start a focus mode, ending the previous one if active."""
    fm = get_focus_modes(system)
    now_iso = datetime.now(timezone.utc).isoformat()
    if fm["current"]:
        last = fm["current"]
        fm["history"].append({"name": last["name"], "started": last["started"], "ended": now_iso})
        if len(fm["history"]) > 500:
            fm["history"] = fm["history"][-500:]
    fm["current"] = {"name": mode_name, "started": now_iso}

def end_focus_mode(system):
    """End the current focus mode. Returns True if a mode was active."""
    fm = get_focus_modes(system)
    if not fm["current"]:
        return False
    now_iso = datetime.now(timezone.utc).isoformat()
    last = fm["current"]
    fm["history"].append({"name": last["name"], "started": last["started"], "ended": now_iso})
    if len(fm["history"]) > 500:
        fm["history"] = fm["history"][-500:]
    fm["current"] = None
    return True

def calc_mode_stats(system, days: int = 30):
    """Returns dict[mode_name -> total_seconds] for the last N days."""
    fm = get_focus_modes(system)
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    totals: dict = {}
    for entry in fm.get("history", []):
        try:
            started = datetime.fromisoformat(entry["started"])
            ended = datetime.fromisoformat(entry["ended"])
        except (KeyError, ValueError):
            continue
        if ended < cutoff:
            continue
        effective_start = max(started, cutoff)
        secs = (ended - effective_start).total_seconds()
        totals[entry["name"]] = totals.get(entry["name"], 0) + secs
    cur = fm.get("current")
    if cur:
        try:
            started = datetime.fromisoformat(cur["started"])
        except (KeyError, ValueError):
            started = datetime.now(timezone.utc)
        effective_start = max(started, cutoff)
        secs = (datetime.now(timezone.utc) - effective_start).total_seconds()
        totals[cur["name"]] = totals.get(cur["name"], 0) + secs
    return totals

def format_playlist_link(url):
    """Render playlist URL as a clean hyperlink label for embeds."""
    if not url:
        return "None"

    cleaned = str(url).strip()
    lowered = cleaned.lower()
    if "music.youtube.com" in lowered:
        label = "Open Playlist on YouTube Music"
    elif "youtube.com" in lowered or "youtu.be" in lowered:
        label = "Open Playlist on YouTube"
    else:
        label = "Open Playlist"

    return f"[{label}]({cleaned})"


def normalize_embed_image_url(value):
    """Normalize image URLs used in embeds and return None when invalid."""
    raw = str(value or "").strip()
    if not raw:
        return None
    if raw.startswith("<") and raw.endswith(">"):
        raw = raw[1:-1].strip()
    if raw.startswith("//"):
        raw = "https:" + raw

    lowered = raw.lower()
    if lowered.startswith("http://") or lowered.startswith("https://"):
        return raw
    return None


def is_ephemeral_discord_attachment_url(value):
    """Return True when a Discord attachment URL is from the ephemeral attachment host/path."""
    raw = str(value or "").lower()
    return "ephemeral-attachments" in raw

def build_member_profile_embed(member, system=None):
    """Build a member profile embed."""
    bio_text = str(member.get("description") or "No description.")

    try:
        embed_color = int(str(member.get("color", "FFFFFF")).lstrip("#"), 16)
    except (TypeError, ValueError):
        embed_color = int("00DE9B", 16)

    tags = ", ".join(member.get("tags", [])) if member.get("tags") else "None"
    proxy_text = render_member_proxy_result(member)
    fronting = "Yes" if member.get("fronting") else "No"
    groups_text = format_member_group_lines(system, member) if system is not None else "None"
    if len(groups_text) > 600:
        groups_text = groups_text[:597] + "..."
    playlist_text = format_playlist_link(member["yt_playlist"]) if member.get("yt_playlist") else "None"

    summary_lines = [
        f"**ID:** {member['id']}",
        f"**Pronouns:** {member.get('pronouns', 'Unknown')}",
        f"**Birthday:** {member.get('birthday', 'Unknown')}",
        f"**Tags:** {tags}",
        f"**Proxy Tag:** {proxy_text}",
        f"**Groups:** {groups_text}",
        f"**Playlist:** {playlist_text}",
        f"**Currently Fronting:** {fronting}",
    ]

    summary_text = "\n".join(summary_lines)

    # Combine summary + bio into one description block separated by a rule
    # This prevents awkward line splits on mobile between fields
    bio_limit = 1000
    if len(bio_text) > bio_limit:
        bio_text = bio_text[:bio_limit - 3] + "..."
    bio_section = bio_text or "No description."

    full_description = f"{summary_text}\n\n**About**\n{bio_section}"
    if len(full_description) > 4000:
        full_description = full_description[:3997] + "..."

    embed = discord.Embed(
        title=member["name"],
        description=full_description,
        color=embed_color
    )

    profile_pic_url = normalize_embed_image_url(member.get("profile_pic"))
    if profile_pic_url:
        embed.set_thumbnail(url=profile_pic_url)

    banner_url = normalize_embed_image_url(member.get("banner"))
    if banner_url:
        embed.set_image(url=banner_url)

    created_iso = member.get("created_at")
    if created_iso:
        try:
            created_dt = datetime.fromisoformat(created_iso)
            created_formatted = created_dt.strftime("%B %d, %Y")
            embed.set_footer(text=f"Created At: {created_formatted}")
        except Exception:
            pass

    return embed

def build_subsystem_card_embed(subsystem_data, subsystem_id, system):
    """Build a subsystem profile embed."""
    desc = subsystem_data.get("description", "No description set.")
    subsystem_name = subsystem_data.get("subsystem_name", "Unnamed Subsystem")
    
    try:
        embed_color = int(str(subsystem_data.get("color", "00DE9B")).lstrip("#"), 16)
    except (TypeError, ValueError):
        embed_color = int("00DE9B", 16)
    
    embed = discord.Embed(
        title=f"{subsystem_name} - Subsystem Card",
        description=desc,
        color=embed_color
    )
    
    embed.add_field(name="Subsystem ID", value=f"`{subsystem_id}`", inline=True)
    
    member_count = len(subsystem_data.get("members", {}))
    embed.add_field(name="Members", value=str(member_count), inline=True)
    
    if subsystem_data.get("profile_pic"):
        embed.set_thumbnail(url=subsystem_data["profile_pic"])
    if subsystem_data.get("banner"):
        embed.set_image(url=subsystem_data["banner"])
    
    if system:
        system_name = system.get("system_name", "Unnamed System")
        embed.add_field(name="Parent System", value=system_name, inline=True)
    
    return embed

def get_external_settings(system):
    """Return and initialize external messaging settings for a system."""
    settings = system.setdefault("external_messages", {})
    settings.setdefault("accept", False)
    settings.setdefault("blocked_users", [])
    settings.setdefault("muted_users", [])
    settings.setdefault("trusted_only", False)
    settings.setdefault("trusted_users", [])
    settings.setdefault("temp_blocks", {})
    settings.setdefault("pending_requests", [])
    settings.setdefault("audit_log", [])
    settings.setdefault("message_max_length", 1500)
    settings.setdefault("inbox_retention_days", 30)
    settings.setdefault("target_rate_seconds", EXTERNAL_TARGET_LIMIT_SECONDS)
    settings.setdefault("quiet_hours", {"enabled": False, "start": 23, "end": 7})
    settings.setdefault("delivery_mode", "public")
    # Normalize values as strings for stable comparisons.
    settings["blocked_users"] = [str(uid) for uid in settings.get("blocked_users", [])]
    settings["muted_users"] = [str(uid) for uid in settings.get("muted_users", [])]
    settings["trusted_users"] = [str(uid) for uid in settings.get("trusted_users", [])]
    settings["temp_blocks"] = {str(k): v for k, v in settings.get("temp_blocks", {}).items()}
    return settings

def add_external_audit_entry(system, action, sender_user_id=None, target_member_id=None, details=None):
    settings = get_external_settings(system)
    log = settings.setdefault("audit_log", [])
    log.append({
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "action": action,
        "sender_user_id": str(sender_user_id) if sender_user_id is not None else None,
        "target_member_id": str(target_member_id) if target_member_id is not None else None,
        "details": details,
    })
    if len(log) > EXTERNAL_AUDIT_MAX:
        settings["audit_log"] = log[-EXTERNAL_AUDIT_MAX:]

def is_in_quiet_hours(system, now_utc=None):
    settings = get_external_settings(system)
    quiet = settings.get("quiet_hours", {})
    if not quiet.get("enabled"):
        return False
    if now_utc is None:
        now_utc = datetime.now(timezone.utc)
    local_now = now_utc.astimezone(get_system_timezone(system))
    hour = local_now.hour
    start = int(quiet.get("start", 23))
    end = int(quiet.get("end", 7))
    if start == end:
        return False
    if start < end:
        return start <= hour < end
    return hour >= start or hour < end

def prune_external_temp_blocks(settings):
    now = datetime.now(timezone.utc)
    updated = {}
    for uid, until_iso in settings.get("temp_blocks", {}).items():
        try:
            until_dt = datetime.fromisoformat(until_iso)
        except (TypeError, ValueError):
            continue
        if until_dt > now:
            updated[str(uid)] = until_iso
    settings["temp_blocks"] = updated

def cleanup_external_inbox_entries(system):
    settings = get_external_settings(system)
    retention_days = int(settings.get("inbox_retention_days", 30))
    cutoff = datetime.now(timezone.utc) - timedelta(days=retention_days)
    changed = False

    for _, members_dict in iter_system_member_dicts(system):
        for member in members_dict.values():
            inbox = member.get("inbox", [])
            if not inbox:
                continue
            kept = []
            for msg in inbox:
                if not msg.get("external"):
                    kept.append(msg)
                    continue
                ts = msg.get("sent_at")
                if not ts:
                    changed = True
                    continue
                try:
                    dt = datetime.fromisoformat(ts)
                except ValueError:
                    changed = True
                    continue
                if dt >= cutoff:
                    kept.append(msg)
                else:
                    changed = True
            if len(kept) != len(inbox):
                member["inbox"] = kept
    return changed

def check_and_update_external_target_rate_limit(sender_user_id, target_user_id, target_seconds):
    now = datetime.now(timezone.utc)
    key = f"{sender_user_id}:{target_user_id}"
    last = external_target_rate_state.get(key)
    if last and (now - last).total_seconds() < target_seconds:
        return False
    external_target_rate_state[key] = now
    return True

def format_inbox_entry_for_channel(msg):
    """Format inbox entry for channel-safe display."""
    sender = msg.get("from", "Unknown")
    text = msg.get("text", "")
    if msg.get("external"):
        sender_id = msg.get("from_user_id", "unknown")
        return f"> From {sender} | User ID: {sender_id}: {text}"
    return f"> From {sender}: {text}"

def format_inbox_entry_for_dm(msg):
    """Format inbox entry for private DM display with safety metadata."""
    sender = msg.get("from", "Unknown")
    text = msg.get("text", "")
    if msg.get("external"):
        sender_id = msg.get("from_user_id", "unknown")
        sender_system_id = msg.get("from_system_id", "unknown")
        return (
            f"From: {sender}\n"
            f"Sender User ID: {sender_id}\n"
            f"Sender System ID: {sender_system_id}\n"
            f"Message: {text}"
        )
    return f"From: {sender}\nMessage: {text}"

def check_and_update_external_rate_limit(sender_user_id):
    """Return True if sender is within rate limit and record this send."""
    now = datetime.now(timezone.utc)
    key = str(sender_user_id)
    recent = external_msg_rate_state.get(key, [])
    cutoff = now - timedelta(seconds=EXTERNAL_MSG_LIMIT_SECONDS)
    recent = [ts for ts in recent if ts >= cutoff]

    if len(recent) >= EXTERNAL_MSG_LIMIT_COUNT:
        external_msg_rate_state[key] = recent
        return False

    recent.append(now)
    external_msg_rate_state[key] = recent
    return True

def parse_discord_user_id(raw_value):
    """Accept raw IDs or mentions like <@123> / <@!123> and return numeric ID string."""
    if raw_value is None:
        return None
    value = str(raw_value).strip()
    if value.startswith("<@") and value.endswith(">"):
        value = value[2:-1]
        if value.startswith("!"):
            value = value[1:]
    return value if value.isdigit() else None


def normalize_profile_privacy_level(value, default="private"):
    cleaned = str(value or "").strip().lower()
    return cleaned if cleaned in PROFILE_PRIVACY_LEVELS else default


def get_system_privacy_level(system):
    level = normalize_profile_privacy_level(system.get("system_privacy"), default="private")
    system.setdefault("system_privacy", level)
    return level


def get_member_privacy_level(member):
    level = normalize_profile_privacy_level(member.get("privacy_level"), default="private")
    member.setdefault("privacy_level", level)
    return level


def can_view_with_privacy(level, system, viewer_user_id):
    owner_id = str(system.get("owner_id") or "")
    viewer_id = str(viewer_user_id)
    if viewer_id == owner_id:
        return True
    if level == "public":
        return True
    if level == "trusted":
        trusted = set(str(uid) for uid in get_external_settings(system).get("trusted_users", []))
        return viewer_id in trusted
    return False


def can_view_system_data(system, viewer_user_id):
    return can_view_with_privacy(get_system_privacy_level(system), system, viewer_user_id)


def can_view_member_data(system, member, viewer_user_id):
    return can_view_with_privacy(get_member_privacy_level(member), system, viewer_user_id)


def resolve_target_system_for_view(requester_user_id, target_user_id_raw=None):
    if target_user_id_raw is None:
        target_user_id = str(requester_user_id)
    else:
        parsed = parse_discord_user_id(target_user_id_raw)
        if not parsed:
            return None, None, None, "Invalid target user ID. Use a numeric Discord ID or mention."
        target_user_id = parsed

    system_id = get_user_system_id(target_user_id)
    if not system_id:
        return None, None, None, "Target user does not have a registered system."

    system = systems_data.get("systems", {}).get(system_id)
    if not system:
        return None, None, None, "System not found."

    return system_id, system, target_user_id, None


def parse_time_delta(time_str):
    """Parse time string like '2hrs', '30mins', '1day' and return timedelta or None."""
    time_str = time_str.strip().lower()
    
    # Try various formats
    formats = [
        (r'^(\d+)\s*h(?:ours?|rs?)?$', 'hours'),
        (r'^(\d+)\s*m(?:in(?:utes?)?|s)?$', 'minutes'),
        (r'^(\d+)\s*d(?:ays?)?$', 'days'),
        (r'^(\d+)\s*s(?:ec(?:onds?)?)?$', 'seconds'),
    ]
    
    import re
    for pattern, unit in formats:
        match = re.match(pattern, time_str)
        if match:
            value = int(match.group(1))
            if unit == 'hours':
                return timedelta(hours=value)
            elif unit == 'minutes':
                return timedelta(minutes=value)
            elif unit == 'days':
                return timedelta(days=value)
            elif unit == 'seconds':
                return timedelta(seconds=value)
    return None


def parse_sendmessage_args(args_str):
    """Parse 'target:future time:2hrs message:\"your message here\"' format.
    Returns (target, time_delta, message, error_msg)
    """
    if not args_str:
        return None, None, None, "No arguments provided."
    
    # Extract parts using a simple parser
    parts = {}
    import re
    
    # Match target:value, time:value, and message:"quoted value"
    pattern = r'(\w+):(?:"([^"]*)"|(\S+))'
    
    matches = re.findall(pattern, args_str)
    for key, quoted_val, unquoted_val in matches:
        value = quoted_val if quoted_val else unquoted_val
        parts[key.lower()] = value
    
    # Validate required fields
    target = parts.get('target', '').lower()
    if target != 'future':
        return None, None, None, "Target must be 'future' (currently only self-messaging supported)."
    
    time_str = parts.get('time', '')
    if not time_str:
        return None, None, None, "Missing 'time' parameter (e.g., time:2hrs)."
    
    time_delta = parse_time_delta(time_str)
    if not time_delta:
        return None, None, None, f"Invalid time format: {time_str}. Use formats like 2hrs, 30mins, 1day."
    
    message = parts.get('message', '').strip()
    if not message:
        return None, None, None, "Missing 'message' parameter."
    
    if len(message) > 2000:
        return None, None, None, "Message is too long (max 2000 characters)."
    
    return 'future', time_delta, message, None


def add_scheduled_message(user_id, message, time_delta):
    """Add a scheduled message for a user."""
    send_at = datetime.now(timezone.utc) + time_delta
    
    if user_id not in SCHEDULED_MESSAGES:
        SCHEDULED_MESSAGES[user_id] = []
    
    SCHEDULED_MESSAGES[user_id].append({
        "send_at": send_at,
        "message": message
    })


def _fetch_pluralkit_members_sync(token: str):
    """Fetch members for the authenticated PluralKit system."""
    request = urllib.request.Request(
        "https://api.pluralkit.me/v2/systems/@me/members",
        headers={
            "Authorization": token,
            "User-Agent": "CortexBot/1.0 (+importpluralkit)"
        },
        method="GET",
    )

    with urllib.request.urlopen(request, timeout=20) as response:
        payload = response.read().decode("utf-8")
        data = json.loads(payload)
        if not isinstance(data, list):
            raise ValueError("Unexpected PluralKit response format.")
        return data


async def fetch_pluralkit_members(token: str):
    """Async wrapper for PluralKit member fetch."""
    return await asyncio.to_thread(_fetch_pluralkit_members_sync, token)


def normalize_proxy_formats(proxy_formats):
    """Normalize proxy formats to a deduplicated list of {prefix, suffix} dicts."""
    normalized = []
    seen = set()

    for item in proxy_formats or []:
        if isinstance(item, dict):
            prefix = (item.get("prefix") or "").strip()
            suffix = (item.get("suffix") or "").strip()
        elif isinstance(item, (list, tuple)) and len(item) >= 2:
            prefix = (item[0] or "").strip()
            suffix = (item[1] or "").strip()
        else:
            continue

        if not prefix and not suffix:
            continue

        key = (prefix, suffix)
        if key in seen:
            continue
        seen.add(key)
        normalized.append({"prefix": prefix or None, "suffix": suffix or None})

    return normalized


def get_pk_proxy_formats(proxy_tags):
    """Return all usable proxy formats from PluralKit proxy tags."""
    if not isinstance(proxy_tags, list):
        return []

    results = []
    for tag in proxy_tags:
        if not isinstance(tag, dict):
            continue
        prefix = (tag.get("prefix") or "").strip()
        suffix = (tag.get("suffix") or "").strip()
        if prefix or suffix:
            results.append({"prefix": prefix or None, "suffix": suffix or None})
    return normalize_proxy_formats(results)


def map_pluralkit_member_to_cortex(pk_member):
    """Map one PluralKit member object to Cortex member fields."""
    name = (pk_member.get("name") or pk_member.get("display_name") or "Unnamed Member").strip()
    display_name = (pk_member.get("display_name") or "").strip() or None
    pronouns = (pk_member.get("pronouns") or "").strip() or None
    birthday = pk_member.get("birthday")
    description = (pk_member.get("description") or "").strip() or None
    
    # Handle profile picture - check avatar_url, then avatar as fallback
    profile_pic = (pk_member.get("avatar_url") or "").strip() or None
    if not profile_pic:
        profile_pic = (pk_member.get("avatar") or "").strip() or None
    
    # Handle banner
    banner = (pk_member.get("banner") or "").strip() or None
    
    proxy_formats = get_pk_proxy_formats(pk_member.get("proxy_tags"))
    proxy_prefix = proxy_formats[0].get("prefix") if proxy_formats else None
    proxy_suffix = proxy_formats[0].get("suffix") if proxy_formats else None
    proxy_tag = proxy_prefix or proxy_suffix
    proxy_tag_position = "suffix" if proxy_suffix and not proxy_prefix else "prefix"

    raw_color = pk_member.get("color")
    color_hex = None
    if raw_color:
        try:
            color_hex = normalize_hex(str(raw_color))
        except ValueError:
            color_hex = None

    return {
        "pk_member_id": str(pk_member.get("id", "")).strip() or None,
        "name": name,
        "display_name": display_name,
        "pronouns": pronouns,
        "birthday": birthday,
        "description": description,
        "profile_pic": profile_pic,
        "banner": banner,
        "color": color_hex,
        "proxy_tag": proxy_tag,
        "proxy_prefix": proxy_prefix,
        "proxy_suffix": proxy_suffix,
        "proxy_tag_position": proxy_tag_position,
        "proxy_formats": proxy_formats,
    }

def resolve_target_member_scope(target_system, target_system_id, target_member_id, target_subsystem_id=None):
    """Resolve target member destination for external messaging.

    Returns (members_dict, scope_id, member_data) or (None, None, None).
    """
    if target_subsystem_id is not None:
        members_dict = get_system_members(target_system_id, target_subsystem_id)
        if not members_dict:
            return None, None, None
        member_data = members_dict.get(target_member_id)
        if not member_data:
            return None, None, None
        return members_dict, target_subsystem_id, member_data

    # No subsystem provided: search main system and all subsystems.
    for scope_id, members_dict in iter_system_member_dicts(target_system):
        member_data = members_dict.get(target_member_id)
        if member_data:
            return members_dict, scope_id, member_data

    return None, None, None

def get_member_from_scope(system, scope_id, member_id):
    """Resolve a member from main system/subsystem scope and member id."""
    if member_id is None:
        return None
    if scope_id is None:
        return system.get("members", {}).get(member_id)
    return system.get("subsystems", {}).get(scope_id, {}).get("members", {}).get(member_id)


def move_member_between_scopes(system_id, member_id, to_subsystem_id=None, from_subsystem_id=None):
    """Move an existing member between main/subsystem scopes.

    Returns (ok, message, old_scope_id, new_scope_id).
    """
    system = systems_data.get("systems", {}).get(system_id)
    if not system:
        return False, "System not found.", None, None

    # Resolve source scope.
    source_scope_id = from_subsystem_id
    source_members = None
    member_obj = None

    if from_subsystem_id is not None:
        source_members = get_system_members(system_id, from_subsystem_id)
        if source_members is None:
            return False, "Source subsystem not found.", None, None
        member_obj = source_members.get(member_id)
        if member_obj is None:
            return False, f"Member not found in {get_scope_label(from_subsystem_id)}.", None, None
    else:
        for scope_id, members_dict in iter_system_member_dicts(system):
            if member_id in members_dict:
                source_scope_id = scope_id
                source_members = members_dict
                member_obj = members_dict[member_id]
                break
        if member_obj is None:
            return False, "Member not found in your system.", None, None

    # Resolve destination scope.
    dest_members = get_system_members(system_id, to_subsystem_id)
    if dest_members is None:
        return False, "Destination subsystem not found.", source_scope_id, None

    if source_scope_id == to_subsystem_id:
        return False, "Source and destination are the same.", source_scope_id, to_subsystem_id

    if member_id in dest_members:
        return False, f"Destination already has a member with ID `{member_id}`.", source_scope_id, to_subsystem_id

    # Move by preserving same member object and ID.
    source_members.pop(member_id, None)
    dest_members[member_id] = member_obj
    return True, "Member moved.", source_scope_id, to_subsystem_id

def find_tagged_proxy_member(system, content):
    """Find the best matching member proxy tag and return (scope_id, member, stripped_text)."""
    best_match = None

    for scope_id, members_dict in iter_system_member_dicts(system):
        for member_data in members_dict.values():
            for proxy_format in get_member_proxy_formats(member_data):
                prefix = proxy_format.get("prefix") or ""
                suffix = proxy_format.get("suffix") or ""
                if prefix and not content.startswith(prefix):
                    continue
                if suffix and not content.endswith(suffix):
                    continue

                end_index = len(content) - len(suffix) if suffix else len(content)
                if end_index < len(prefix):
                    continue

                match_len = len(prefix) + len(suffix)
                # Prefer the most specific (longest) tag format to avoid collisions.
                if best_match is None or match_len > best_match[0]:
                    stripped = content[len(prefix):end_index].strip()
                    best_match = (match_len, scope_id, member_data, stripped)

    if best_match is None:
        return None, None, None

    _, scope_id, member_data, stripped = best_match
    return scope_id, member_data, stripped

def get_member_proxy_formats(member_data):
    """Return all proxy formats for a member with backward compatibility."""
    if not isinstance(member_data, dict):
        return []

    existing_formats = member_data.get("proxy_formats")
    if isinstance(existing_formats, list):
        normalized = normalize_proxy_formats(existing_formats)
        if normalized:
            return normalized

    prefix = member_data.get("proxy_prefix")
    suffix = member_data.get("proxy_suffix")
    if prefix is not None or suffix is not None:
        return normalize_proxy_formats([{"prefix": prefix, "suffix": suffix}])

    legacy_tag = member_data.get("proxy_tag")
    if not legacy_tag:
        return []

    legacy_position = str(member_data.get("proxy_tag_position", "prefix")).lower()
    if legacy_position == "suffix":
        return normalize_proxy_formats([{"prefix": None, "suffix": legacy_tag}])
    return normalize_proxy_formats([{"prefix": legacy_tag, "suffix": None}])


def get_member_proxy_parts(member_data):
    """Return the primary (prefix, suffix) proxy parts for a member."""
    formats = get_member_proxy_formats(member_data)
    if not formats:
        return "", ""

    primary = formats[0]
    return (primary.get("prefix") or ""), (primary.get("suffix") or "")


def set_member_proxy_formats(member_data, proxy_formats):
    """Persist proxy formats and keep legacy proxy fields in sync."""
    normalized = normalize_proxy_formats(proxy_formats)
    member_data["proxy_formats"] = normalized

    if not normalized:
        member_data["proxy_tag"] = None
        member_data["proxy_prefix"] = None
        member_data["proxy_suffix"] = None
        member_data["proxy_tag_position"] = "prefix"
        return

    primary = normalized[0]
    prefix = primary.get("prefix")
    suffix = primary.get("suffix")
    member_data["proxy_prefix"] = prefix
    member_data["proxy_suffix"] = suffix
    member_data["proxy_tag"] = prefix or suffix
    member_data["proxy_tag_position"] = "suffix" if suffix and not prefix else "prefix"


def add_member_proxy_format(member_data, prefix, suffix):
    """Add one proxy format to a member. Returns True if added, False if duplicate."""
    existing = get_member_proxy_formats(member_data)
    candidate = normalize_proxy_formats([{"prefix": prefix, "suffix": suffix}])
    if not candidate:
        return False
    candidate = candidate[0]
    key = (candidate.get("prefix") or "", candidate.get("suffix") or "")
    for current in existing:
        current_key = (current.get("prefix") or "", current.get("suffix") or "")
        if current_key == key:
            return False
    existing.append(candidate)
    set_member_proxy_formats(member_data, existing)
    return True


def remove_member_proxy_format(member_data, prefix, suffix):
    """Remove one proxy format from a member. Returns True if removed."""
    existing = get_member_proxy_formats(member_data)
    target = normalize_proxy_formats([{"prefix": prefix, "suffix": suffix}])
    if not target:
        return False
    target = target[0]
    target_key = (target.get("prefix") or "", target.get("suffix") or "")

    filtered = []
    removed = False
    for current in existing:
        current_key = (current.get("prefix") or "", current.get("suffix") or "")
        if not removed and current_key == target_key:
            removed = True
            continue
        filtered.append(current)

    if removed:
        set_member_proxy_formats(member_data, filtered)
    return removed

def render_member_proxy_format(member_data):
    """Render a member's proxy format with 'text' placeholder for display/help."""
    formats = get_member_proxy_formats(member_data)
    if not formats:
        return "Not set"
    return " | ".join(
        f"{(fmt.get('prefix') or '')}text{(fmt.get('suffix') or '')}"
        for fmt in formats
    )

def render_member_proxy_result(member_data):
    """Render stored proxy parts without the text placeholder."""
    formats = get_member_proxy_formats(member_data)
    if not formats:
        return "Not set"
    lines = []
    for fmt in formats:
        prefix = fmt.get("prefix") or ""
        suffix = fmt.get("suffix") or ""
        if prefix and suffix:
            lines.append(f"Prefix `{prefix}` | Suffix `{suffix}`")
        elif prefix:
            lines.append(f"Prefix `{prefix}`")
        else:
            lines.append(f"Suffix `{suffix}`")
    return "\n".join(lines)

def parse_proxy_format_with_placeholder(raw_format: str):
    """Parse a proxy format containing exactly one 'text' placeholder."""
    raw = (raw_format or "").strip()
    if not raw:
        return None, None, "empty"

    lowered = raw.lower()
    idx = lowered.find("text")
    if idx == -1:
        return None, None, "missing_placeholder"
    if lowered.find("text", idx + 4) != -1:
        return None, None, "multiple_placeholders"

    prefix = raw[:idx]
    suffix = raw[idx + 4:]
    if not prefix and not suffix:
        return None, None, "only_placeholder"

    return prefix, suffix, None

async def get_or_create_proxy_webhook(channel):
    """Get or create a webhook for proxying in this channel/thread."""
    base_channel = channel.parent if isinstance(channel, discord.Thread) else channel
    # Support standard text channels and forum parents for thread proxying.
    if not isinstance(base_channel, (discord.TextChannel, discord.ForumChannel)):
        return None

    webhooks = await base_channel.webhooks()
    webhook = next(
        (w for w in webhooks if w.user and w.user.id == bot.user.id and w.name == PROXY_WEBHOOK_NAME),
        None,
    )
    if webhook is None:
        webhook = await base_channel.create_webhook(name=PROXY_WEBHOOK_NAME)
    return webhook

@tree.command(name="register", description="Register as a system or singlet profile")
@app_commands.choices(mode=[
    app_commands.Choice(name="System", value="system"),
    app_commands.Choice(name="Singlet", value="singlet"),
])
async def register(
    interaction: discord.Interaction,
    system_name: str,
    mode: str = "system"
):
    user_id = interaction.user.id
    if get_user_system_id(user_id):
        await interaction.response.send_message("You already have a registered profile.", ephemeral=True)
        return
    # Find next system id
    next_id = str(max([int(sid) for sid in systems_data["systems"].keys()] or [0]) + 1)
    systems_data["systems"][next_id] = {
        "system_name": system_name,
        "mode": mode,
        "owner_id": str(user_id),
        "system_privacy": "private",
        "members": {},
        "subsystems": {},
        "timezone": "UTC",
        "system_tag": None,
        "system_profile": {
            "collective_pronouns": None,
            "description": None,
            "profile_pic": None,
            "banner": None,
            "color": "00DE9B"
        },
        "checkins": {
            "entries": [],
            "weekly_dm_enabled": True,
            "last_weekly_summary_week": None
        },
        "front_reminders": {
            "enabled": False,
            "hours": 4
        },
        "external_messages": {
            "accept": False,
            "blocked_users": [],
            "muted_users": [],
            "trusted_only": False,
            "trusted_users": [],
            "temp_blocks": {},
            "pending_requests": [],
            "audit_log": [],
            "message_max_length": 1500,
            "inbox_retention_days": 30,
            "target_rate_seconds": EXTERNAL_TARGET_LIMIT_SECONDS,
            "quiet_hours": {"enabled": False, "start": 23, "end": 7},
            "delivery_mode": "private_summary"
        },
        "autoproxy": {
            "mode": "off",
            "latch_scope_id": None,
            "latch_member_id": None
        },
        "autoproxy_server_overrides": {},
        "server_appearance_overrides": {},
        "server_member_overrides": {},
        "group_settings": {
            "groups": {},
            "order": []
        }
    }
    save_systems()
    if mode == "singlet":
        await interaction.response.send_message(
            f"Singlet profile **{system_name}** registered! You can now use external messaging and wellness commands.",
            ephemeral=True
        )
    else:
        await interaction.response.send_message(
            f"System **{system_name}** registered! You can now add members and subsystems.",
            ephemeral=True
        )

# Create subsystem command
@tree.command(name="createsubsystem", description="Create a subsystem under your main system")
async def createsubsystem(
    interaction: discord.Interaction,
    subsystem_name: str
):
    user_id = interaction.user.id
    system_id = get_user_system_id(user_id)
    if not system_id:
        await interaction.response.send_message("You must register a main system first using /register.", ephemeral=True)
        return
    system = systems_data["systems"][system_id]
    subsystems = system.setdefault("subsystems", {})
    next_sub_id = chr(97 + len(subsystems))  # 'a', 'b', 'c', ...
    subsystems[next_sub_id] = {
        "subsystem_name": subsystem_name,
        "members": {},
        "description": None,
        "color": "00DE9B"
    }
    save_systems()
    await interaction.response.send_message(
        f"Subsystem **{subsystem_name}** created with ID `{next_sub_id}`.",
        ephemeral=True
    )


@tree.command(name="editsubsystem", description="Edit a subsystem's name")
async def editsubsystem(
    interaction: discord.Interaction,
    subsystem_id: str,
    new_name: str
):
    user_id = interaction.user.id
    system_id = get_user_system_id(user_id)
    if not system_id:
        await interaction.response.send_message("You must register a main system first using /register.", ephemeral=True)
        return
    
    system = systems_data["systems"][system_id]
    subsystems = system.get("subsystems", {})
    
    if subsystem_id not in subsystems:
        await interaction.response.send_message(f"Subsystem `{subsystem_id}` not found.", ephemeral=True)
        return
    
    old_name = subsystems[subsystem_id].get("subsystem_name", "Unnamed")
    subsystems[subsystem_id]["subsystem_name"] = new_name
    save_systems()
    
    await interaction.response.send_message(
        f"Subsystem `{subsystem_id}` renamed from **{old_name}** to **{new_name}**.",
        ephemeral=True
    )


@tree.command(name="viewsubsystemcard", description="View a subsystem's profile card")
async def viewsubsystemcard(
    interaction: discord.Interaction,
    subsystem_id: str,
    show_to_others: bool = False,
    target_user_id: str = None,
):
    requester_id = interaction.user.id
    system_id, system, target_owner_id, error = resolve_target_system_for_view(requester_id, target_user_id)
    if error:
        await interaction.response.send_message(error, ephemeral=True)
        return

    if str(target_owner_id) != str(requester_id) and not can_view_system_data(system, requester_id):
        await interaction.response.send_message("You do not have permission to view this subsystem card.", ephemeral=True)
        return

    subsystems = system.get("subsystems", {})
    
    if subsystem_id not in subsystems:
        await interaction.response.send_message(f"Subsystem `{subsystem_id}` not found.", ephemeral=True)
        return
    
    subsystem_data = subsystems[subsystem_id]
    embed = build_subsystem_card_embed(subsystem_data, subsystem_id, system)
    
    await interaction.response.send_message(embed=embed, ephemeral=not show_to_others)


@tree.command(name="editsubsystemcard", description="Edit a subsystem's profile card")
async def editsubsystemcard(
    interaction: discord.Interaction,
    subsystem_id: str,
    description: str = None,
    color: str = None,
    profile_pic: discord.Attachment = None,
    banner: discord.Attachment = None,
    clear_profile_pic: bool = False,
    clear_banner: bool = False
):
    user_id = interaction.user.id
    system_id = get_user_system_id(user_id)
    if not system_id:
        await interaction.response.send_message("You must register a main system first using /register.", ephemeral=True)
        return

    system = systems_data["systems"][system_id]
    subsystems = system.get("subsystems", {})
    
    if subsystem_id not in subsystems:
        await interaction.response.send_message(f"Subsystem `{subsystem_id}` not found.", ephemeral=True)
        return
    
    subsystem_data = subsystems[subsystem_id]
    
    if description is not None:
        subsystem_data["description"] = description.strip() or None
    
    if color:
        try:
            subsystem_data["color"] = normalize_hex(color)
        except ValueError as e:
            await interaction.response.send_message(str(e), ephemeral=True)
            return
    
    if clear_profile_pic:
        subsystem_data.pop("profile_pic", None)
    elif profile_pic:
        subsystem_data["profile_pic"] = profile_pic.url
    
    if clear_banner:
        subsystem_data.pop("banner", None)
    elif banner:
        subsystem_data["banner"] = banner.url
    
    save_systems()
    embed = build_subsystem_card_embed(subsystem_data, subsystem_id, system)
    await interaction.response.send_message("Subsystem card updated.", embed=embed, ephemeral=True)


@tree.command(name="listsubsystems", description="List your subsystems with their IDs")
async def listsubsystems(
    interaction: discord.Interaction,
    show_to_others: bool = False,
    target_user_id: str = None,
):
    requester_id = interaction.user.id
    system_id, system, target_owner_id, error = resolve_target_system_for_view(requester_id, target_user_id)
    if error:
        await interaction.response.send_message(error, ephemeral=True)
        return

    if str(target_owner_id) != str(requester_id) and not can_view_system_data(system, requester_id):
        await interaction.response.send_message("You do not have permission to view this subsystem list.", ephemeral=True)
        return

    subsystems = system.get("subsystems", {})

    if not subsystems:
        await interaction.response.send_message("You have no subsystems yet. Use /createsubsystem to add one.", ephemeral=True)
        return

    lines = []
    for sub_id, sub_data in sorted(subsystems.items()):
        sub_name = sub_data.get("subsystem_name", "Unnamed Subsystem")
        member_count = len(sub_data.get("members", {}))
        lines.append(f"ID `{sub_id}` - {sub_name} ({member_count} members)")

    embed = discord.Embed(
        title=f"{system.get('system_name', 'System')} Subsystems",
        description="\n".join(lines),
        color=discord.Color.blurple()
    )
    await interaction.response.send_message(embed=embed, ephemeral=not show_to_others)

# -----------------------------
# System tag
# -----------------------------
@tree.command(name="systemtag", description="Set, clear, or view your system-level proxy tag")
async def systemtag(interaction: discord.Interaction, tag: str = None, clear: bool = False):
    user_id = interaction.user.id
    system_id = get_user_system_id(user_id)
    if not system_id:
        await interaction.response.send_message("You must register a main system first using /register.", ephemeral=True)
        return

    system = systems_data["systems"].get(system_id)
    if not system:
        await interaction.response.send_message("System not found.", ephemeral=True)
        return

    if clear:
        system["system_tag"] = None
        save_systems()
        await interaction.response.send_message("System tag cleared.", ephemeral=True)
        return

    if tag is not None:
        cleaned = tag.strip()
        if not cleaned:
            await interaction.response.send_message("Tag cannot be blank. Use clear=true to remove it.", ephemeral=True)
            return
        system["system_tag"] = cleaned
        save_systems()
        await interaction.response.send_message(f"System tag set to: {cleaned}", ephemeral=True)
        return

    current_tag = get_system_proxy_tag(system)
    if current_tag:
        await interaction.response.send_message(f"Current system tag: {current_tag}", ephemeral=True)
    else:
        await interaction.response.send_message("No system tag set. Use /systemtag tag:<value> to set one.", ephemeral=True)


async def systemprivacy(interaction: discord.Interaction, level: str):
    user_id = interaction.user.id
    system_id = get_user_system_id(user_id)
    if not system_id:
        await interaction.response.send_message("You must register first using /register.", ephemeral=True)
        return

    system = systems_data["systems"].get(system_id)
    if not system:
        await interaction.response.send_message("System not found.", ephemeral=True)
        return

    cleaned = normalize_profile_privacy_level(level)
    system["system_privacy"] = cleaned
    save_systems()
    await interaction.response.send_message(
        f"System privacy set to **{cleaned}**.",
        ephemeral=True,
    )


async def alterprivacy(
    interaction: discord.Interaction,
    member_id: str,
    level: str,
    subsystem_id: str = None,
):
    user_id = interaction.user.id
    system_id = get_user_system_id(user_id)
    if not system_id:
        await interaction.response.send_message("You must register first using /register.", ephemeral=True)
        return

    members_dict = get_system_members(system_id, subsystem_id)
    if members_dict is None:
        await interaction.response.send_message("Subsystem not found.", ephemeral=True)
        return
    if member_id not in members_dict:
        await interaction.response.send_message(f"Member not found in {get_scope_label(subsystem_id)}.", ephemeral=True)
        return

    member = members_dict[member_id]
    cleaned = normalize_profile_privacy_level(level)
    member["privacy_level"] = cleaned
    save_systems()
    await interaction.response.send_message(
        f"Privacy for **{member.get('name', member_id)}** set to **{cleaned}**.",
        ephemeral=True,
    )


async def privacystatus(interaction: discord.Interaction):
    user_id = interaction.user.id
    system_id = get_user_system_id(user_id)
    if not system_id:
        await interaction.response.send_message("You must register first using /register.", ephemeral=True)
        return

    system = systems_data["systems"].get(system_id)
    if not system:
        await interaction.response.send_message("System not found.", ephemeral=True)
        return

    counts = {"private": 0, "trusted": 0, "public": 0}
    total_members = 0
    sample_lines = []
    for scope_id, members_dict in iter_system_member_dicts(system):
        for member_id, member in members_dict.items():
            level = get_member_privacy_level(member)
            counts[level] = counts.get(level, 0) + 1
            total_members += 1
            if len(sample_lines) < 12:
                sample_lines.append(f"• {member.get('name', member_id)} (`{member_id}`) — {level} ({get_scope_label(scope_id)})")

    embed = discord.Embed(
        title="Privacy Status",
        color=discord.Color.blurple(),
        description=(
            f"System privacy: **{get_system_privacy_level(system)}**\n"
            f"Trusted users: **{len(get_external_settings(system).get('trusted_users', []))}**"
        ),
    )
    embed.add_field(
        name="Alter Privacy Summary",
        value=(
            f"Private: **{counts.get('private', 0)}**\n"
            f"Trusted: **{counts.get('trusted', 0)}**\n"
            f"Public: **{counts.get('public', 0)}**\n"
            f"Total alters: **{total_members}**"
        ),
        inline=False,
    )
    embed.add_field(
        name="Sample Alters",
        value="\n".join(sample_lines) if sample_lines else "No alters found.",
        inline=False,
    )
    await interaction.response.send_message(embed=embed, ephemeral=True)

@tree.command(name="viewsystemcard", description="View your system profile card")
async def viewsystemcard(
    interaction: discord.Interaction,
    show_to_others: bool = False,
    target_user_id: str = None,
):
    requester_id = interaction.user.id
    system_id, system, target_owner_id, error = resolve_target_system_for_view(requester_id, target_user_id)
    if error:
        await interaction.response.send_message(error, ephemeral=True)
        return

    if str(target_owner_id) != str(requester_id) and not can_view_system_data(system, requester_id):
        await interaction.response.send_message("You do not have permission to view this system card.", ephemeral=True)
        return

    profile = get_system_profile(system)
    try:
        embed_color = int(str(profile.get("color", "00DE9B")).lstrip("#"), 16)
    except (TypeError, ValueError):
        embed_color = int("00DE9B", 16)

    embed = discord.Embed(
        title=f"{system.get('system_name', 'Unnamed System')} - System Card",
        description=profile.get("description") or "No description set.",
        color=embed_color
    )
    embed.add_field(name="Mode", value=system.get("mode", "system").title(), inline=True)
    embed.add_field(name="Collective Pronouns", value=profile.get("collective_pronouns") or "Not set", inline=True)
    embed.add_field(name="System Tag", value=get_system_proxy_tag(system) or "Not set", inline=True)

    if profile.get("profile_pic"):
        embed.set_thumbnail(url=profile["profile_pic"])
    if profile.get("banner"):
        embed.set_image(url=profile["banner"])

    await interaction.response.send_message(embed=embed, ephemeral=not show_to_others)

@tree.command(name="editsystemcard", description="Edit your system profile card details")
async def editsystemcard(
    interaction: discord.Interaction,
    system_name: str = None,
    collective_pronouns: str = None,
    description: str = None,
    color: str = None,
    profile_pic: discord.Attachment = None,
    banner: discord.Attachment = None,
    clear_profile_pic: bool = False,
    clear_banner: bool = False
):
    user_id = interaction.user.id
    system_id = get_user_system_id(user_id)
    if not system_id:
        await interaction.response.send_message("You must register first using /register.", ephemeral=True)
        return

    system = systems_data["systems"].get(system_id)
    if not system:
        await interaction.response.send_message("System not found.", ephemeral=True)
        return

    profile = get_system_profile(system)

    if system_name:
        system["system_name"] = system_name
    if collective_pronouns is not None:
        profile["collective_pronouns"] = collective_pronouns.strip() or None
    if description is not None:
        profile["description"] = description.strip() or None
    if color:
        try:
            profile["color"] = normalize_hex(color)
        except ValueError as e:
            await interaction.response.send_message(str(e), ephemeral=True)
            return

    if clear_profile_pic:
        profile["profile_pic"] = None
    elif profile_pic:
        profile["profile_pic"] = profile_pic.url

    if clear_banner:
        profile["banner"] = None
    elif banner:
        profile["banner"] = banner.url

    save_systems()
    await interaction.response.send_message("System card updated.", ephemeral=True)

@tree.command(name="serveridentity", description="Set server-specific system tag, display name, or icon for this server")
async def serveridentity(
    interaction: discord.Interaction,
    display_name: str = None,
    system_tag: str = None,
    profile_pic: discord.Attachment = None,
    clear_display_name: bool = False,
    clear_system_tag: bool = False,
    clear_profile_pic: bool = False,
):
    if interaction.guild_id is None:
        await interaction.response.send_message("This command can only be used in a server.", ephemeral=True)
        return

    user_id = interaction.user.id
    system_id = get_user_system_id(user_id)
    if not system_id:
        await interaction.response.send_message("You must register a main system first using /register.", ephemeral=True)
        return

    system = systems_data["systems"].get(system_id)
    if not system:
        await interaction.response.send_message("System not found.", ephemeral=True)
        return

    guild_key = str(interaction.guild_id)
    overrides = system.setdefault("server_appearance_overrides", {})
    server_app = overrides.setdefault(guild_key, {})
    changed = []

    if clear_display_name:
        server_app.pop("display_name", None)
        changed.append("Server display name cleared.")
    elif display_name is not None:
        val = display_name.strip()
        server_app["display_name"] = val or None
        changed.append(f"Server display name set to **{val}**." if val else "Server display name cleared.")

    if clear_system_tag:
        server_app.pop("system_tag", None)
        changed.append("Server system tag cleared.")
    elif system_tag is not None:
        val = system_tag.strip()
        server_app["system_tag"] = val or None
        changed.append(f"Server system tag set to **{val}**." if val else "Server system tag cleared.")

    if clear_profile_pic:
        server_app.pop("profile_pic", None)
        changed.append("Server icon cleared.")
    elif profile_pic is not None:
        server_app["profile_pic"] = profile_pic.url
        changed.append("Server icon updated.")

    if not server_app:
        overrides.pop(guild_key, None)

    if not changed:
        current = get_server_appearance(system, interaction.guild_id)
        lines = [
            "**Server identity overrides for this server:**",
            f"Display name: **{current.get('display_name') if current else None or 'none (uses global)'}**",
            f"System tag: **{current.get('system_tag') if current else None or 'none (uses global)'}**",
            f"Icon: {'set' if (current and current.get('profile_pic')) else 'none (uses global)'}",
        ]
        await interaction.response.send_message("\n".join(lines), ephemeral=True)
        return

    save_systems()
    await interaction.response.send_message("\n".join(changed), ephemeral=True)

@tree.command(name="serveridentitystatus", description="View effective system tag, display name, and icon for this server")
async def serveridentitystatus(interaction: discord.Interaction):
    user_id = interaction.user.id
    system_id = get_user_system_id(user_id)
    if not system_id:
        await interaction.response.send_message("You must register a main system first using /register.", ephemeral=True)
        return

    system = systems_data["systems"].get(system_id)
    if not system:
        await interaction.response.send_message("System not found.", ephemeral=True)
        return

    guild_id = interaction.guild_id
    server_app = get_server_appearance(system, guild_id) if guild_id else None
    global_tag = system.get("system_tag")
    global_name = system.get("system_name", "Unnamed System")
    global_profile = get_system_profile(system)
    global_icon = global_profile.get("profile_pic")

    eff_name = (server_app.get("display_name") if server_app else None) or global_name
    eff_tag = (server_app.get("system_tag") if server_app else None) or global_tag
    eff_icon = (server_app.get("profile_pic") if server_app else None) or global_icon

    lines = [
        "**Display name:**",
        f"  Global: {global_name}",
        f"  Server override: {(server_app.get('display_name') if server_app else None) or 'none'}",
        f"  **Effective: {eff_name}**",
        "",
        "**System tag:**",
        f"  Global: {global_tag or 'not set'}",
        f"  Server override: {(server_app.get('system_tag') if server_app else None) or 'none'}",
        f"  **Effective: {eff_tag or 'not set'}**",
        "",
        "**Icon:**",
        f"  Global: {'set' if global_icon else 'not set'}",
        f"  Server override: {'set' if (server_app and server_app.get('profile_pic')) else 'none'}",
        f"  **Effective: {'set' if eff_icon else 'not set'}**",
    ]
    await interaction.response.send_message("\n".join(lines), ephemeral=True)

@tree.command(name="servermemberidentity", description="Set server-specific identity overrides for one member")
async def servermemberidentity(
    interaction: discord.Interaction,
    member_id: str,
    subsystem_id: str = None,
    display_name: str = None,
    system_tag: str = None,
    profile_pic: discord.Attachment = None,
    clear_display_name: bool = False,
    clear_system_tag: bool = False,
    clear_profile_pic: bool = False,
):
    if interaction.guild_id is None:
        await interaction.response.send_message("This command can only be used in a server.", ephemeral=True)
        return

    user_id = interaction.user.id
    system_id = get_user_system_id(user_id)
    if not system_id:
        await interaction.response.send_message("You must register a main system first using /register.", ephemeral=True)
        return

    system = systems_data["systems"].get(system_id)
    if not system:
        await interaction.response.send_message("System not found.", ephemeral=True)
        return

    resolved_scope_id, member, error_key = resolve_member_for_override(system, member_id, subsystem_id=subsystem_id)
    if error_key == "subsystem_not_found":
        await interaction.response.send_message("Subsystem not found.", ephemeral=True)
        return
    if error_key == "ambiguous":
        await interaction.response.send_message(
            "Member ID exists in multiple scopes. Provide `subsystem_id` to target the correct member.",
            ephemeral=True,
        )
        return
    if error_key == "not_found" or not member:
        await interaction.response.send_message(f"Member not found in {get_scope_label(subsystem_id)}.", ephemeral=True)
        return

    guild_key = str(interaction.guild_id)
    by_guild = system.setdefault("server_member_overrides", {}).setdefault(guild_key, {})
    member_key = str(member.get("id") or member_id)
    override = by_guild.setdefault(member_key, {})
    changed = []

    if clear_display_name:
        override.pop("display_name", None)
        changed.append("Member server display name cleared.")
    elif display_name is not None:
        val = display_name.strip()
        override["display_name"] = val or None
        changed.append(f"Member server display name set to **{val}**." if val else "Member server display name cleared.")

    if clear_system_tag:
        override.pop("system_tag", None)
        changed.append("Member server tag suffix cleared.")
    elif system_tag is not None:
        val = system_tag.strip()
        override["system_tag"] = val or None
        changed.append(f"Member server tag suffix set to **{val}**." if val else "Member server tag suffix cleared.")

    if clear_profile_pic:
        override.pop("profile_pic", None)
        changed.append("Member server icon cleared.")
    elif profile_pic is not None:
        override["profile_pic"] = profile_pic.url
        changed.append("Member server icon updated.")

    if not override:
        by_guild.pop(member_key, None)
    if not by_guild:
        system.setdefault("server_member_overrides", {}).pop(guild_key, None)

    if not changed:
        current = get_server_member_appearance(system, interaction.guild_id, member_key)
        lines = [
            f"**Server member identity overrides for {member.get('name', 'Unknown')} (`{member_key}`):**",
            f"Display name: **{(current.get('display_name') if current else None) or 'none (uses member default)'}**",
            f"Tag suffix: **{(current.get('system_tag') if current else None) or 'none (uses server/global system tag)'}**",
            f"Icon: {'set' if (current and current.get('profile_pic')) else 'none (uses member/system default)'}",
        ]
        await interaction.response.send_message("\n".join(lines), ephemeral=True)
        return

    save_systems()
    await interaction.response.send_message("\n".join(changed), ephemeral=True)

@tree.command(name="servermemberidentitystatus", description="View effective server-specific identity values for one member")
async def servermemberidentitystatus(
    interaction: discord.Interaction,
    member_id: str,
    subsystem_id: str = None,
):
    user_id = interaction.user.id
    system_id = get_user_system_id(user_id)
    if not system_id:
        await interaction.response.send_message("You must register a main system first using /register.", ephemeral=True)
        return

    system = systems_data["systems"].get(system_id)
    if not system:
        await interaction.response.send_message("System not found.", ephemeral=True)
        return

    resolved_scope_id, member, error_key = resolve_member_for_override(system, member_id, subsystem_id=subsystem_id)
    if error_key == "subsystem_not_found":
        await interaction.response.send_message("Subsystem not found.", ephemeral=True)
        return
    if error_key == "ambiguous":
        await interaction.response.send_message(
            "Member ID exists in multiple scopes. Provide `subsystem_id` to target the correct member.",
            ephemeral=True,
        )
        return
    if error_key == "not_found" or not member:
        await interaction.response.send_message(f"Member not found in {get_scope_label(subsystem_id)}.", ephemeral=True)
        return

    guild_id = interaction.guild_id
    member_key = str(member.get("id") or member_id)
    member_override = get_server_member_appearance(system, guild_id, member_key) if guild_id else None
    system_override = get_server_appearance(system, guild_id) if guild_id else None
    system_profile = get_system_profile(system)

    base_display = member.get("display_name") or member.get("name", "Unknown")
    eff_display = (member_override.get("display_name") if member_override else None) or base_display

    base_tag = get_system_proxy_tag(system)
    system_level_tag = (system_override.get("system_tag") if system_override else None) or base_tag
    eff_tag = (member_override.get("system_tag") if member_override else None) or system_level_tag

    base_icon = member.get("profile_pic") or system_profile.get("profile_pic")
    system_level_icon = (system_override.get("profile_pic") if system_override else None) or base_icon
    eff_icon = (member_override.get("profile_pic") if member_override else None) or system_level_icon

    lines = [
        f"**Member:** {member.get('name', 'Unknown')} (`{member_key}`) in {get_scope_label(resolved_scope_id)}",
        "",
        "**Display name:**",
        f"  Member default: {base_display}",
        f"  Server member override: {(member_override.get('display_name') if member_override else None) or 'none'}",
        f"  Effective: **{eff_display}**",
        "",
        "**Tag suffix:**",
        f"  Global/system default: {base_tag or 'not set'}",
        f"  Server system override: {(system_override.get('system_tag') if system_override else None) or 'none'}",
        f"  Server member override: {(member_override.get('system_tag') if member_override else None) or 'none'}",
        f"  Effective: **{eff_tag or 'not set'}**",
        "",
        "**Icon:**",
        f"  Member/system default: {'set' if base_icon else 'not set'}",
        f"  Server system override: {'set' if (system_override and system_override.get('profile_pic')) else 'none'}",
        f"  Server member override: {'set' if (member_override and member_override.get('profile_pic')) else 'none'}",
        f"  Effective: **{'set' if eff_icon else 'not set'}**",
    ]
    await interaction.response.send_message("\n".join(lines), ephemeral=True)

# -----------------------------
# Load members
# -----------------------------
if os.path.exists(JSON_FILE):
    with open(JSON_FILE, "r") as f:
        members = json.load(f)
else:
    members = {}

# -----------------------------
# Load or create tags file
# -----------------------------
if not os.path.exists(TAGS_FILE):
    _gh_tags, _ = _github_get_file(TAGS_FILE)
    if _gh_tags:
        PRESET_TAGS = _gh_tags
        with open(TAGS_FILE, "w") as f:
            json.dump(PRESET_TAGS, f, indent=4)
    else:
        PRESET_TAGS = []
        with open(TAGS_FILE, "w") as f:
            json.dump(PRESET_TAGS, f, indent=4)
else:
    with open(TAGS_FILE, "r") as f:
        PRESET_TAGS = json.load(f)

def save_tags():
    with open(TAGS_FILE, "w") as f:
        json.dump(PRESET_TAGS, f, indent=4)
    _github_save_file(TAGS_FILE, PRESET_TAGS)

def save_members():
    with open(JSON_FILE, "w") as f:
        json.dump(members, f, indent=4)
    _github_save_file(JSON_FILE, members)

def normalize_hex(hex_code: str):
    if not hex_code:
        return None
    hex_code = hex_code.strip().lstrip("#")
    if len(hex_code) != 6 or any(c not in "0123456789ABCDEFabcdef" for c in hex_code):
        raise ValueError("Color must be a 6-digit HEX code (e.g., FF0000)")
    return hex_code.upper()

def get_next_member_id():
    if not members:
        return 1
    return max(int(mid) for mid in members.keys()) + 1

# -----------------------------
# Autocomplete functions
# -----------------------------
async def member_name_autocomplete(interaction: discord.Interaction, current: str):
    try:
        options = []
        for m in members.values():
            if current.lower() in m["name"].lower():
                options.append(app_commands.Choice(name=f"{m['name']} ({m['id']})", value=m["name"]))
            if len(options) >= 25:
                break
        return options
    except Exception:
        return []

async def member_id_autocomplete(interaction: discord.Interaction, current: str):
    options = []
    for m in members.values():
        if current in str(m["id"]):
            options.append(app_commands.Choice(name=f"{m['name']} ({m['id']})", value=str(m["id"])))
        if len(options) >= 25:
            break
    return options

async def switchmember_member_id_autocomplete(interaction: discord.Interaction, current: str):
    """Autocomplete for switchmember member_id that's aware of subsystem_id parameter."""
    try:
        user_id = interaction.user.id
        system_id = get_user_system_id(user_id)
        if not system_id:
            return []
        
        # Get subsystem_id from the interaction namespace if it was already filled in
        subsystem_id = interaction.namespace.subsystem_id if hasattr(interaction.namespace, 'subsystem_id') else None
        
        # Get members from the appropriate scope
        members_dict = get_system_members(system_id, subsystem_id)
        if not members_dict:
            return []
        
        options = []
        for member_id, member_data in members_dict.items():
            if current.lower() in member_id.lower() or current.lower() in member_data.get("name", "").lower():
                name = member_data.get("name", "Unknown")
                options.append(app_commands.Choice(name=f"{name} ({member_id})", value=member_id))
            if len(options) >= 25:
                break
        return options
    except Exception:
        return []

async def subsystem_id_autocomplete(interaction: discord.Interaction, current: str):
    try:
        user_id = interaction.user.id
        system_id = get_user_system_id(user_id)
        if not system_id:
            return []

        system = systems_data.get("systems", {}).get(system_id, {})
        subsystems = system.get("subsystems", {})

        options = []
        current_lower = current.lower()
        for sub_id, sub_data in sorted(subsystems.items()):
            sub_name = sub_data.get("subsystem_name", "Unnamed Subsystem")
            if current_lower in sub_id.lower() or current_lower in sub_name.lower():
                label = f"{sub_name} ({sub_id})"
                options.append(app_commands.Choice(name=label[:100], value=sub_id))
            if len(options) >= 25:
                break
        return options
    except Exception:
        return []

async def tag_autocomplete(interaction: discord.Interaction, current: str):
    try:
        options = []
        for tag in PRESET_TAGS:
            if current.lower() in tag.lower():
                options.append(app_commands.Choice(name=tag, value=tag))
            if len(options) >= 25:
                break
        return options
    except Exception:
        return []

# -----------------------------
# Tag selection views
# -----------------------------
class TagSelect(discord.ui.Select):
    def __init__(self, preselected=None):
        options = [
            discord.SelectOption(label=tag, value=tag, default=(tag in preselected if preselected else False))
            for tag in PRESET_TAGS
        ][:25]
        super().__init__(
            placeholder="Select tags (up to 25 shown)...",
            min_values=0,
            max_values=len(options) if options else 1,
            options=options
        )

    async def callback(self, interaction: discord.Interaction):
        self.view.selected_tags = self.values
        await interaction.response.defer()

class ConfirmTags(discord.ui.Button):
    def __init__(self):
        super().__init__(label="Confirm", style=discord.ButtonStyle.green)

    async def callback(self, interaction: discord.Interaction):
        self.view.stop()
        await interaction.response.defer()

class TagView(discord.ui.View):
    def __init__(self, preselected=None):
        super().__init__(timeout=120)
        self.selected_tags = preselected or []
        if PRESET_TAGS:
            self.add_item(TagSelect(preselected))
        self.add_item(ConfirmTags())

class TagMultiSelect(discord.ui.Select):
    def __init__(self):
        options = [discord.SelectOption(label=tag, value=tag) for tag in PRESET_TAGS[:25]]
        super().__init__(
            placeholder="Select one or more tags (up to 25 shown)...",
            min_values=1,
            max_values=len(options) if options else 1,
            options=options
        )

    async def callback(self, interaction: discord.Interaction):
        tag_list = self.values
        filtered = [m for m in members.values() if all(t in m.get("tags", []) for t in tag_list)]
        if not filtered:
            desc = "No members match all selected tags."
        else:
            desc = "\n".join(f"**{m['name']}** — ID `{m['id']}` — Tags: {', '.join(m.get('tags', []))}" for m in filtered)
        embed = discord.Embed(title=f"Members matching tags: {', '.join(tag_list)}", description=desc, color=discord.Color.green())
        await interaction.response.edit_message(embed=embed, view=self.view)

class TagMultiView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)
        if PRESET_TAGS:
            self.add_item(TagMultiSelect())
# -----------------------------
# Fronting helpers (unified)
# -----------------------------
def format_us(iso_string):
    """Format an ISO datetime string to US-friendly format."""
    try:
        dt = datetime.fromisoformat(iso_string)
        return dt.strftime("%m/%d/%Y %I:%M %p")
    except Exception:
        return iso_string

def start_front(member_id, cofronts=None, members_dict=None):
    """Start fronting for a member with optional co-fronts."""
    if cofronts is None:
        cofronts = []
    if members_dict is None:
        members_dict = members

    member = members_dict[member_id]
    now_iso = datetime.now(timezone.utc).isoformat()

    # End any existing front
    if member.get("current_front"):
        end_front(member_id, members_dict=members_dict)

    # Set current front
    member["current_front"] = {
        "start": now_iso,
        "cofronts": cofronts,
        "status": None,
        "reminder_sent": False,
        "reminded_at": None,
    }

    # Ensure front history exists
    member.setdefault("front_history", [])
    # Add new session to history (end=None for ongoing)
    member["front_history"].append({"start": now_iso, "end": None, "cofronts": cofronts})

def end_front(member_id, members_dict=None):
    """End current front session for a member and record duration."""
    if members_dict is None:
        members_dict = members
    
    member = members_dict[member_id]
    current = member.get("current_front")
    if not current:
        return

    start_iso = current["start"]
    start_dt = datetime.fromisoformat(start_iso)
    end_dt = datetime.now(timezone.utc)

    # Update last front history entry
    if member.get("front_history"):
        member["front_history"][-1]["end"] = end_dt.isoformat()

    # Reset current front
    member["current_front"] = None

# -----------------------------
# Front duration helpers (unified)
# -----------------------------


def calculate_front_duration(member):
    """Returns total front duration in seconds, including current front if active, including co-fronts."""
    total_seconds = 0

    for entry in member.get("front_history", []):
        start_iso = entry.get("start")
        end_iso = entry.get("end")
        if not start_iso:
            continue
        start_dt = datetime.fromisoformat(start_iso)
        end_dt = datetime.fromisoformat(end_iso) if end_iso else datetime.now(timezone.utc)
        total_seconds += (end_dt - start_dt).total_seconds()

    # Include current front
    current = member.get("current_front")
    if current:
        start_dt = datetime.fromisoformat(current["start"])
        total_seconds += (datetime.now(timezone.utc) - start_dt).total_seconds()

    return total_seconds


def format_duration(seconds):
    """Formats seconds as Hh Mm Ss"""
    seconds = int(seconds)
    td = timedelta(seconds=seconds)
    hours, remainder = divmod(td.seconds, 3600)
    minutes, secs = divmod(remainder, 60)
    total_hours = td.days * 24 + hours
    if total_hours:
        return f"{total_hours}h {minutes}m {secs}s"
    return f"{minutes}m {secs}s"

def time_of_day_bucket(hour):
    """Map hour (0-23) to a coarse time-of-day bucket."""
    if 5 <= hour < 12:
        return "Morning"
    if 12 <= hour < 17:
        return "Afternoon"
    if 17 <= hour < 22:
        return "Evening"
    return "Night"

# -----------------------------
# Co-front UI
# -----------------------------
class CoFrontSelect(discord.ui.Select):
    def __init__(self, parent_view):
        self.parent_view = parent_view
        options = self.parent_view.build_page_options()
        super().__init__(
            placeholder="Select co-front members...",
            min_values=0,
            max_values=len(options) if options else 1,
            options=options
        )

    async def callback(self, interaction: discord.Interaction):
        page_member_ids = {opt.value for opt in self.options}
        self.parent_view.selected_cofronts.difference_update(page_member_ids)
        self.parent_view.selected_cofronts.update(self.values)
        await interaction.response.defer()

class CoFrontView(discord.ui.View):
    def __init__(self, members_dict, main_member_id):
        super().__init__(timeout=120)
        self.main_member_id = str(main_member_id)
        self.member_items = sorted(
            [
                (str(member_id), member)
                for member_id, member in members_dict.items()
                if str(member_id) != self.main_member_id
            ],
            key=lambda item: item[1].get("name", "").lower()
        )
        self.page_size = 25
        self.current_page = 0
        self.total_pages = max(1, (len(self.member_items) - 1) // self.page_size + 1)
        self.selected_cofronts = set()
        self.cancelled = False

        self.cofront_select = CoFrontSelect(self)
        self.add_item(self.cofront_select)

    def current_page_items(self):
        start = self.current_page * self.page_size
        end = start + self.page_size
        return self.member_items[start:end]

    def build_page_options(self):
        options = []
        for member_id, member in self.current_page_items():
            options.append(
                discord.SelectOption(
                    label=member.get("name", "Unknown")[:100],
                    value=member_id,
                    default=(member_id in self.selected_cofronts)
                )
            )
        return options

    async def refresh_message(self, interaction: discord.Interaction):
        self.remove_item(self.cofront_select)
        self.cofront_select = CoFrontSelect(self)
        self.add_item(self.cofront_select)
        await interaction.response.edit_message(
            content=(
                f"Select co-front members, then click Confirm. "
                f"Page {self.current_page + 1}/{self.total_pages}. "
                f"Selected: {len(self.selected_cofronts)}"
            ),
            view=self
        )

    @discord.ui.button(label="Previous", style=discord.ButtonStyle.secondary)
    async def prev_page(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self.current_page > 0:
            self.current_page -= 1
        await self.refresh_message(interaction)

    @discord.ui.button(label="Next", style=discord.ButtonStyle.secondary)
    async def next_page(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self.current_page < self.total_pages - 1:
            self.current_page += 1
        await self.refresh_message(interaction)

    @discord.ui.button(label="Confirm", style=discord.ButtonStyle.green)
    async def confirm(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.stop()
        await interaction.response.defer()

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.danger)
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.cancelled = True
        self.selected_cofronts.clear()
        self.stop()
        await interaction.response.edit_message(content="Action cancelled.", view=None)

# -----------------------------
# Add member
# -----------------------------
@tree.command(name="addmember", description="Add a member to a subsystem")
@app_commands.autocomplete(subsystem_id=subsystem_id_autocomplete)
async def addmember(
    interaction: discord.Interaction,
    name: str,
    displayname: str = None,
    subsystem_id: str = None,
    pronouns: str = None,
    birthday: str = None,
    description: str = None,
    profile_pic: discord.Attachment = None,
    banner: discord.Attachment = None,
    yt_playlist: str = None,
    color: str = None,
    proxy_tag: str = None
):
    user_id = interaction.user.id
    system_id = get_user_system_id(user_id)
    if not system_id:
        await interaction.response.send_message("You must register a main system first using /register.", ephemeral=True)
        return
    members = get_system_members(system_id, subsystem_id)
    if members is None:
        await interaction.response.send_message("Subsystem not found. Please check your subsystem ID.", ephemeral=True)
        return
    try:
        color_hex = normalize_hex(color)
    except ValueError as e:
        await interaction.response.send_message(str(e), ephemeral=True)
        return
    if not color_hex:
        color_hex = "%06x" % random.randint(0, 0xFFFFFF)

    # Show interactive tag selector
    view = TagView()
    await interaction.response.send_message("Select tags then press Confirm.", view=view, ephemeral=True)
    await view.wait()
    tags_list = view.selected_tags

    # Save new tags if any
    for tag in tags_list:
        if tag not in PRESET_TAGS:
            PRESET_TAGS.append(tag)
    save_tags()

    member_id = get_next_system_member_id(system_id)
    members[member_id] = {
        "id": member_id,
        "name": name,
        "display_name": (displayname.strip() if displayname else None),
        "pronouns": pronouns,
        "birthday": birthday,
        "description": description,
        "profile_pic": profile_pic.url if profile_pic else None,
        "banner": banner.url if banner else None,
        "yt_playlist": yt_playlist,
        "tags": tags_list,
        "groups": [],
        "color": color_hex,
        "proxy_tag": proxy_tag,
        "proxy_formats": normalize_proxy_formats([{"prefix": proxy_tag, "suffix": None}]) if proxy_tag else [],
        "autoproxy": False,
        "privacy_level": "private",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "fronting": False,
        "cofronting": [],
        "fronting_since": None,
        "front_history": []
    }
    save_systems()
    await interaction.followup.send(f"Member **{name}** added to {get_scope_label(subsystem_id)}.\nID `{member_id}`", ephemeral=True)


@tree.command(name="movemember", description="Move an existing member between main system and subsystems")
@app_commands.autocomplete(from_subsystem_id=subsystem_id_autocomplete)
@app_commands.autocomplete(to_subsystem_id=subsystem_id_autocomplete)
async def movemember(
    interaction: discord.Interaction,
    member_id: str,
    to_subsystem_id: str = None,
    from_subsystem_id: str = None,
):
    user_id = interaction.user.id
    system_id = get_user_system_id(user_id)
    if not system_id:
        await interaction.response.send_message("You must register a main system first using /register.", ephemeral=True)
        return

    ok, message, old_scope, new_scope = move_member_between_scopes(
        system_id,
        member_id,
        to_subsystem_id=to_subsystem_id,
        from_subsystem_id=from_subsystem_id,
    )
    if not ok:
        await interaction.response.send_message(message, ephemeral=True)
        return

    save_systems()
    await interaction.response.send_message(
        f"Moved member `{member_id}` from {get_scope_label(old_scope)} to {get_scope_label(new_scope)}.",
        ephemeral=True,
    )


@tree.command(name="importpluralkit", description="Import members from your PluralKit system")
@app_commands.autocomplete(subsystem_id=subsystem_id_autocomplete)
@app_commands.describe(
    token="Your PluralKit token (used once, never saved)",
    subsystem_id="Optional subsystem to import into (default: main system)",
    overwrite_existing="If true, update matched existing members",
    dry_run="If true, preview changes without saving"
)
async def importpluralkit(
    interaction: discord.Interaction,
    token: str,
    subsystem_id: str = None,
    overwrite_existing: bool = False,
    dry_run: bool = False,
):
    user_id = interaction.user.id
    system_id = get_user_system_id(user_id)
    if not system_id:
        await interaction.response.send_message("You must register a main system first using /register.", ephemeral=True)
        return

    members = get_system_members(system_id, subsystem_id)
    if members is None:
        await interaction.response.send_message("Subsystem not found. Please check your subsystem ID.", ephemeral=True)
        return

    await interaction.response.defer(ephemeral=True)

    token_value = (token or "").strip()
    if not token_value:
        await interaction.followup.send("Please provide a valid PluralKit token.", ephemeral=True)
        return

    try:
        pk_members = await fetch_pluralkit_members(token_value)
    except urllib.error.HTTPError as e:
        if e.code in (401, 403):
            await interaction.followup.send("PluralKit rejected that token. Please check it and try again.", ephemeral=True)
        else:
            await interaction.followup.send(f"PluralKit API error ({e.code}). Please try again shortly.", ephemeral=True)
        return
    except (urllib.error.URLError, TimeoutError):
        await interaction.followup.send("Could not reach PluralKit right now. Please try again shortly.", ephemeral=True)
        return
    except Exception:
        await interaction.followup.send("Import failed due to an unexpected response from PluralKit.", ephemeral=True)
        return

    if not pk_members:
        await interaction.followup.send("No members found in your PluralKit system.", ephemeral=True)
        return

    members_working = deepcopy(members) if dry_run else members

    existing_by_pk_id = {}
    existing_by_key = {}
    for member_id, member in members_working.items():
        pk_member_id = member.get("pk_member_id")
        if pk_member_id:
            existing_by_pk_id[str(pk_member_id)] = (member_id, member)
        for key in get_member_lookup_keys(member):
            if key not in existing_by_key:
                existing_by_key[key] = (member_id, member)

    next_member_num = int(get_next_system_member_id(system_id))

    imported_count = 0
    updated_count = 0
    skipped_count = 0
    failed_count = 0

    for raw_pk_member in pk_members:
        try:
            mapped = map_pluralkit_member_to_cortex(raw_pk_member)
            pk_member_id = mapped.get("pk_member_id")
            matched = existing_by_pk_id.get(pk_member_id) if pk_member_id else None

            if matched:
                if not overwrite_existing:
                    skipped_count += 1
                    continue

                _, existing_member = matched
                existing_member["pk_member_id"] = pk_member_id
                existing_member["name"] = mapped["name"]
                existing_member["display_name"] = mapped["display_name"]
                existing_member["pronouns"] = mapped["pronouns"]
                existing_member["birthday"] = mapped["birthday"]
                existing_member["description"] = mapped["description"]
                existing_member["profile_pic"] = mapped["profile_pic"]
                existing_member["banner"] = mapped["banner"]
                set_member_proxy_formats(existing_member, mapped.get("proxy_formats", []))
                existing_member.setdefault("privacy_level", "private")
                if mapped.get("color"):
                    existing_member["color"] = mapped["color"]

                if pk_member_id:
                    existing_by_pk_id[str(pk_member_id)] = matched
                for key in get_member_lookup_keys(existing_member):
                    existing_by_key[key] = matched
                updated_count += 1
                continue

            member_id = f"{next_member_num:05d}"
            next_member_num += 1
            members_working[member_id] = {
                "id": member_id,
                "pk_member_id": pk_member_id,
                "name": mapped["name"],
                "display_name": mapped["display_name"],
                "pronouns": mapped["pronouns"],
                "birthday": mapped["birthday"],
                "description": mapped["description"],
                "profile_pic": mapped["profile_pic"],
                "banner": mapped["banner"],
                "yt_playlist": None,
                "tags": [],
                "groups": [],
                "color": mapped.get("color") or ("%06x" % random.randint(0, 0xFFFFFF)),
                "proxy_tag": mapped["proxy_tag"],
                "proxy_prefix": mapped.get("proxy_prefix"),
                "proxy_suffix": mapped.get("proxy_suffix"),
                "proxy_tag_position": mapped.get("proxy_tag_position", "prefix"),
                "proxy_formats": mapped.get("proxy_formats", []),
                "autoproxy": False,
                "privacy_level": "private",
                "created_at": datetime.now(timezone.utc).isoformat(),
                "fronting": False,
                "cofronting": [],
                "fronting_since": None,
                "front_history": []
            }

            created_member = members_working[member_id]
            if pk_member_id:
                existing_by_pk_id[str(pk_member_id)] = (member_id, created_member)
            for key in get_member_lookup_keys(created_member):
                existing_by_key[key] = (member_id, created_member)
            imported_count += 1
        except Exception:
            failed_count += 1

    if not dry_run:
        save_systems()

    scope_label = get_scope_label(subsystem_id)
    mode_label = "Dry Run (no changes saved)" if dry_run else "Import Complete"
    await interaction.followup.send(
        f"**{mode_label}**\n"
        f"Target: {scope_label}\n"
        f"PluralKit members scanned: **{len(pk_members)}**\n"
        f"Imported new: **{imported_count}**\n"
        f"Updated existing: **{updated_count}**\n"
        f"Skipped existing: **{skipped_count}**\n"
        f"Failed: **{failed_count}**\n\n"
        f"Tip: run again with `dry_run:false` to apply changes.",
        ephemeral=True
    )

# -----------------------------
# Message to member
# Switch member
# -----------------------------
@tree.command(name="messageto", description="Send a message to a member in a subsystem")
@app_commands.autocomplete(subsystem_id=subsystem_id_autocomplete)
async def messageto(interaction: discord.Interaction, member_id: str, message: str, subsystem_id: str = None):
    user_id = interaction.user.id
    system_id = get_user_system_id(user_id)
    if not system_id:
        await interaction.response.send_message("You must register using /register.", ephemeral=True)
        return
    
    members_dict = get_system_members(system_id, subsystem_id)
    if members_dict is None:
        await interaction.response.send_message("Subsystem not found.", ephemeral=True)
        return

    if member_id not in members_dict:
        await interaction.response.send_message(f"Member not found in {get_scope_label(subsystem_id)}.", ephemeral=True)
        return

    # Find who is currently fronting to use as sender
    sender_names = [m["name"] for m in members_dict.values() if m.get("current_front")]
    sender = ", ".join(sender_names) if sender_names else "Unknown"

    # Add message to the member's inbox
    members_dict[member_id].setdefault("inbox", [])
    members_dict[member_id]["inbox"].append({
        "from": sender,
        "text": message,
        "sent_at": datetime.now(timezone.utc).isoformat()
    })
    save_systems()

    await interaction.response.send_message(
        f"Message sent to **{members_dict[member_id]['name']}**.",
        ephemeral=True
    )

# -----------------------------
# Cross-system inbox (Phase 1)
# -----------------------------
@tree.command(name="allowexternal", description="Enable or disable receiving messages from other systems")
async def allowexternal(interaction: discord.Interaction, enabled: bool):
    user_id = interaction.user.id
    system_id = get_user_system_id(user_id)
    if not system_id:
        await interaction.response.send_message("You must register using /register.", ephemeral=True)
        return

    system = systems_data["systems"].get(system_id)
    if not system:
        await interaction.response.send_message("System not found.", ephemeral=True)
        return

    settings = get_external_settings(system)
    settings["accept"] = enabled
    save_systems()
    status = "enabled" if enabled else "disabled"
    await interaction.response.send_message(f"External messages are now **{status}**.", ephemeral=True)

@tree.command(name="externalprivacy", description="Set how external inbox messages are shown when switching")
@app_commands.choices(mode=[
    app_commands.Choice(name="Public (show in switch output)", value="public"),
    app_commands.Choice(name="Private Summary (count in channel, details in DM)", value="private_summary"),
    app_commands.Choice(name="DM Only (details only in DM)", value="dm_only"),
])
async def externalprivacy(interaction: discord.Interaction, mode: str):
    user_id = interaction.user.id
    system_id = get_user_system_id(user_id)
    if not system_id:
        await interaction.response.send_message("You must register using /register.", ephemeral=True)
        return

    system = systems_data["systems"].get(system_id)
    if not system:
        await interaction.response.send_message("System not found.", ephemeral=True)
        return

    settings = get_external_settings(system)
    settings["delivery_mode"] = mode
    save_systems()
    await interaction.response.send_message(f"External delivery mode set to **{mode}**.", ephemeral=True)

@tree.command(name="externalstatus", description="View external messaging safety settings")
async def externalstatus(interaction: discord.Interaction, show_to_others: bool = False):
    user_id = interaction.user.id
    system_id = get_user_system_id(user_id)
    if not system_id:
        await interaction.response.send_message("You must register using /register.", ephemeral=True)
        return

    system = systems_data["systems"].get(system_id)
    if not system:
        await interaction.response.send_message("System not found.", ephemeral=True)
        return

    settings = get_external_settings(system)
    prune_external_temp_blocks(settings)
    save_systems()
    enabled = "enabled" if settings.get("accept") else "disabled"
    mode = settings.get("delivery_mode", "public")
    blocked_count = len(settings.get("blocked_users", []))
    muted_count = len(settings.get("muted_users", []))
    trusted_only = "enabled" if settings.get("trusted_only") else "disabled"
    trusted_count = len(settings.get("trusted_users", []))
    pending_count = len(settings.get("pending_requests", []))
    temp_count = len(settings.get("temp_blocks", {}))
    quiet = settings.get("quiet_hours", {})
    quiet_label = f"{quiet.get('start', 23)}-{quiet.get('end', 7)}" if quiet.get("enabled") else "off"
    await interaction.response.send_message(
        f"External messages: **{enabled}**\n"
        f"Delivery mode: **{mode}**\n"
        f"Trusted-only: **{trusted_only}** (trusted: {trusted_count}, pending: {pending_count})\n"
        f"Blocked users: **{blocked_count}** | Muted users: **{muted_count}** | Temp blocks: **{temp_count}**\n"
        f"Quiet hours: **{quiet_label}**\n"
        f"Limits: max chars **{settings.get('message_max_length', 1500)}**, target cooldown **{settings.get('target_rate_seconds', EXTERNAL_TARGET_LIMIT_SECONDS)}s**, retention **{settings.get('inbox_retention_days', 30)}d**",
        ephemeral=not show_to_others
    )

async def externallimits(interaction: discord.Interaction, max_chars: int = 1500, target_cooldown_seconds: int = EXTERNAL_TARGET_LIMIT_SECONDS):
    user_id = interaction.user.id
    system_id = get_user_system_id(user_id)
    if not system_id:
        await interaction.response.send_message("You must register using /register.", ephemeral=True)
        return
    if max_chars < 50 or max_chars > 4000:
        await interaction.response.send_message("max_chars must be between 50 and 4000.", ephemeral=True)
        return
    if target_cooldown_seconds < 0 or target_cooldown_seconds > 3600:
        await interaction.response.send_message("target_cooldown_seconds must be between 0 and 3600.", ephemeral=True)
        return
    system = systems_data["systems"].get(system_id)
    settings = get_external_settings(system)
    settings["message_max_length"] = max_chars
    settings["target_rate_seconds"] = target_cooldown_seconds
    save_systems()
    await interaction.response.send_message(
        f"External limits updated: max_chars={max_chars}, target cooldown={target_cooldown_seconds}s.",
        ephemeral=True
    )

async def tempblockedusers(interaction: discord.Interaction):
    user_id = interaction.user.id
    system_id = get_user_system_id(user_id)
    if not system_id:
        await interaction.response.send_message("You must register using /register.", ephemeral=True)
        return
    system = systems_data["systems"].get(system_id)
    settings = get_external_settings(system)
    prune_external_temp_blocks(settings)
    blocks = settings.get("temp_blocks", {})
    save_systems()
    if not blocks:
        await interaction.response.send_message("No active temporary blocks.", ephemeral=True)
        return
    lines = [f"- {uid} until {until}" for uid, until in blocks.items()]
    await interaction.response.send_message("Temporary blocks:\n" + "\n".join(lines), ephemeral=True)

async def externaltrustedonly(interaction: discord.Interaction, enabled: bool):
    user_id = interaction.user.id
    system_id = get_user_system_id(user_id)
    if not system_id:
        await interaction.response.send_message("You must register using /register.", ephemeral=True)
        return
    system = systems_data["systems"].get(system_id)
    if not system:
        await interaction.response.send_message("System not found.", ephemeral=True)
        return
    settings = get_external_settings(system)
    settings["trusted_only"] = enabled
    save_systems()
    state = "enabled" if enabled else "disabled"
    await interaction.response.send_message(f"Trusted-senders-only mode is now **{state}**.", ephemeral=True)

async def trustuser(interaction: discord.Interaction, user_id: str):
    owner_id = interaction.user.id
    system_id = get_user_system_id(owner_id)
    if not system_id:
        await interaction.response.send_message("You must register using /register.", ephemeral=True)
        return
    parsed = parse_discord_user_id(user_id)
    if not parsed:
        await interaction.response.send_message("Invalid user ID. Use a numeric Discord ID or mention.", ephemeral=True)
        return
    system = systems_data["systems"].get(system_id)
    settings = get_external_settings(system)
    trusted = settings.get("trusted_users", [])
    if parsed not in trusted:
        trusted.append(parsed)
        settings["trusted_users"] = trusted
        save_systems()
    await interaction.response.send_message(f"Trusted user added: `{parsed}`.", ephemeral=True)

async def untrustuser(interaction: discord.Interaction, user_id: str):
    owner_id = interaction.user.id
    system_id = get_user_system_id(owner_id)
    if not system_id:
        await interaction.response.send_message("You must register using /register.", ephemeral=True)
        return
    parsed = parse_discord_user_id(user_id)
    if not parsed:
        await interaction.response.send_message("Invalid user ID. Use a numeric Discord ID or mention.", ephemeral=True)
        return
    system = systems_data["systems"].get(system_id)
    settings = get_external_settings(system)
    trusted = settings.get("trusted_users", [])
    if parsed in trusted:
        trusted.remove(parsed)
        settings["trusted_users"] = trusted
        save_systems()
    await interaction.response.send_message(f"Trusted user removed: `{parsed}`.", ephemeral=True)

async def trustedusers(interaction: discord.Interaction):
    user_id = interaction.user.id
    system_id = get_user_system_id(user_id)
    if not system_id:
        await interaction.response.send_message("You must register using /register.", ephemeral=True)
        return
    system = systems_data["systems"].get(system_id)
    trusted = get_external_settings(system).get("trusted_users", [])
    if not trusted:
        await interaction.response.send_message("No trusted users.", ephemeral=True)
        return
    await interaction.response.send_message("Trusted users:\n" + "\n".join([f"- {u}" for u in trusted]), ephemeral=True)

@tree.command(name="muteuser", description="Mute a Discord user for external messages (quietly ignore)")
async def muteuser(interaction: discord.Interaction, user_id: str):
    owner_id = interaction.user.id
    system_id = get_user_system_id(owner_id)
    if not system_id:
        await interaction.response.send_message("You must register using /register.", ephemeral=True)
        return
    parsed = parse_discord_user_id(user_id)
    if not parsed:
        await interaction.response.send_message("Invalid user ID. Use a numeric Discord ID or mention.", ephemeral=True)
        return
    system = systems_data["systems"].get(system_id)
    settings = get_external_settings(system)
    muted = settings.get("muted_users", [])
    if parsed not in muted:
        muted.append(parsed)
        settings["muted_users"] = muted
        save_systems()
    await interaction.response.send_message(f"Muted user ID `{parsed}`.", ephemeral=True)

@tree.command(name="unmuteuser", description="Unmute a Discord user for external messages")
async def unmuteuser(interaction: discord.Interaction, user_id: str):
    owner_id = interaction.user.id
    system_id = get_user_system_id(owner_id)
    if not system_id:
        await interaction.response.send_message("You must register using /register.", ephemeral=True)
        return
    parsed = parse_discord_user_id(user_id)
    if not parsed:
        await interaction.response.send_message("Invalid user ID. Use a numeric Discord ID or mention.", ephemeral=True)
        return
    system = systems_data["systems"].get(system_id)
    settings = get_external_settings(system)
    muted = settings.get("muted_users", [])
    if parsed in muted:
        muted.remove(parsed)
        settings["muted_users"] = muted
        save_systems()
    await interaction.response.send_message(f"Unmuted user ID `{parsed}`.", ephemeral=True)

async def mutedusers(interaction: discord.Interaction):
    user_id = interaction.user.id
    system_id = get_user_system_id(user_id)
    if not system_id:
        await interaction.response.send_message("You must register using /register.", ephemeral=True)
        return
    system = systems_data["systems"].get(system_id)
    muted = get_external_settings(system).get("muted_users", [])
    if not muted:
        await interaction.response.send_message("No muted users.", ephemeral=True)
        return
    await interaction.response.send_message("Muted users:\n" + "\n".join([f"- {u}" for u in muted]), ephemeral=True)

@tree.command(name="tempblockuser", description="Temporarily block a user from external messaging")
async def tempblockuser(interaction: discord.Interaction, user_id: str, hours: int = 24):
    owner_id = interaction.user.id
    system_id = get_user_system_id(owner_id)
    if not system_id:
        await interaction.response.send_message("You must register using /register.", ephemeral=True)
        return
    parsed = parse_discord_user_id(user_id)
    if not parsed:
        await interaction.response.send_message("Invalid user ID. Use a numeric Discord ID or mention.", ephemeral=True)
        return
    if hours < 1 or hours > 720:
        await interaction.response.send_message("Hours must be between 1 and 720.", ephemeral=True)
        return
    system = systems_data["systems"].get(system_id)
    settings = get_external_settings(system)
    until = (datetime.now(timezone.utc) + timedelta(hours=hours)).isoformat()
    settings.setdefault("temp_blocks", {})[parsed] = until
    save_systems()
    await interaction.response.send_message(f"Temporarily blocked `{parsed}` for {hours} hour(s).", ephemeral=True)

async def externalpending(interaction: discord.Interaction):
    user_id = interaction.user.id
    system_id = get_user_system_id(user_id)
    if not system_id:
        await interaction.response.send_message("You must register using /register.", ephemeral=True)
        return
    system = systems_data["systems"].get(system_id)
    pending = get_external_settings(system).get("pending_requests", [])
    if not pending:
        await interaction.response.send_message("No pending external requests.", ephemeral=True)
        return
    counts = {}
    for p in pending:
        sid = str(p.get("sender_user_id"))
        counts[sid] = counts.get(sid, 0) + 1
    lines = [f"- {sid}: {count} message(s)" for sid, count in counts.items()]
    await interaction.response.send_message("Pending sender queue:\n" + "\n".join(lines), ephemeral=True)

async def approveexternal(interaction: discord.Interaction, user_id: str, approve: bool = True):
    owner_id = interaction.user.id
    system_id = get_user_system_id(owner_id)
    if not system_id:
        await interaction.response.send_message("You must register using /register.", ephemeral=True)
        return
    parsed = parse_discord_user_id(user_id)
    if not parsed:
        await interaction.response.send_message("Invalid user ID. Use a numeric Discord ID or mention.", ephemeral=True)
        return
    system = systems_data["systems"].get(system_id)
    if not system:
        await interaction.response.send_message("System not found.", ephemeral=True)
        return

    settings = get_external_settings(system)
    pending = settings.get("pending_requests", [])
    matched = [p for p in pending if str(p.get("sender_user_id")) == parsed]
    remaining = [p for p in pending if str(p.get("sender_user_id")) != parsed]
    settings["pending_requests"] = remaining

    delivered = 0
    if approve and matched:
        trusted = settings.get("trusted_users", [])
        if parsed not in trusted:
            trusted.append(parsed)
            settings["trusted_users"] = trusted
        for p in matched:
            members_dict = get_system_members(system_id, p.get("target_subsystem_id"))
            target_member_id = p.get("target_member_id")
            if members_dict is None or target_member_id not in members_dict:
                continue
            members_dict[target_member_id].setdefault("inbox", []).append({
                "from": p.get("from"),
                "from_user_id": str(p.get("sender_user_id")),
                "from_system_id": str(p.get("from_system_id")),
                "text": p.get("text"),
                "sent_at": p.get("sent_at") or datetime.now(timezone.utc).isoformat(),
                "external": True
            })
            delivered += 1

    add_external_audit_entry(system, "approveexternal" if approve else "rejectexternal", parsed, details=f"processed={len(matched)}, delivered={delivered}")
    save_systems()
    action = "approved" if approve else "rejected"
    await interaction.response.send_message(
        f"{action.title()} sender `{parsed}`. Processed {len(matched)} queued message(s), delivered {delivered}.",
        ephemeral=True
    )

async def recentexternal(interaction: discord.Interaction, limit: int = 10):
    user_id = interaction.user.id
    system_id = get_user_system_id(user_id)
    if not system_id:
        await interaction.response.send_message("You must register using /register.", ephemeral=True)
        return
    if limit < 1 or limit > 25:
        await interaction.response.send_message("Limit must be between 1 and 25.", ephemeral=True)
        return
    system = systems_data["systems"].get(system_id)
    log = get_external_settings(system).get("audit_log", [])[-limit:]
    if not log:
        await interaction.response.send_message("No recent external events.", ephemeral=True)
        return
    lines = []
    for entry in reversed(log):
        ts = entry.get("timestamp", "?")
        action = entry.get("action", "?")
        sender = entry.get("sender_user_id") or "-"
        details = entry.get("details") or ""
        lines.append(f"- {ts} | {action} | sender={sender} {details}")
    await interaction.response.send_message("Recent external events:\n" + "\n".join(lines), ephemeral=True)

@tree.command(name="externalquiethours", description="Configure quiet hours for external DM detail delivery")
async def externalquiethours(interaction: discord.Interaction, enabled: bool, start_hour: int = 23, end_hour: int = 7):
    user_id = interaction.user.id
    system_id = get_user_system_id(user_id)
    if not system_id:
        await interaction.response.send_message("You must register using /register.", ephemeral=True)
        return
    if start_hour < 0 or start_hour > 23 or end_hour < 0 or end_hour > 23:
        await interaction.response.send_message("Hours must be 0-23.", ephemeral=True)
        return
    system = systems_data["systems"].get(system_id)
    settings = get_external_settings(system)
    settings["quiet_hours"] = {"enabled": enabled, "start": start_hour, "end": end_hour}
    save_systems()
    await interaction.response.send_message(
        f"External quiet hours updated: enabled={enabled}, start={start_hour}, end={end_hour}.",
        ephemeral=True
    )

@tree.command(name="externalretention", description="Set external inbox retention period in days")
async def externalretention(interaction: discord.Interaction, days: int):
    user_id = interaction.user.id
    system_id = get_user_system_id(user_id)
    if not system_id:
        await interaction.response.send_message("You must register using /register.", ephemeral=True)
        return
    if days < 1 or days > 365:
        await interaction.response.send_message("Days must be between 1 and 365.", ephemeral=True)
        return
    system = systems_data["systems"].get(system_id)
    settings = get_external_settings(system)
    settings["inbox_retention_days"] = days
    save_systems()
    await interaction.response.send_message(f"External retention set to {days} day(s).", ephemeral=True)

@tree.command(name="reportexternal", description="Report abuse from an external sender")
async def reportexternal(interaction: discord.Interaction, user_id: str, reason: str):
    reporter_id = interaction.user.id
    parsed = parse_discord_user_id(user_id)
    if not parsed:
        await interaction.response.send_message("Invalid user ID. Use a numeric Discord ID or mention.", ephemeral=True)
        return
    if len(reason.strip()) < 5:
        await interaction.response.send_message("Please provide a slightly longer reason.", ephemeral=True)
        return

    state = get_moderation_state()
    reports = state.setdefault("reports", [])
    report_id = f"R{len(reports) + 1:05d}"
    reports.append({
        "id": report_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "reporter_user_id": str(reporter_id),
        "accused_user_id": parsed,
        "reason": reason.strip(),
        "status": "open",
        "resolution": None,
    })
    add_mod_event("report_created", target_user_id=parsed, moderator_user_id=reporter_id, details=report_id)
    save_systems()

    await interaction.response.send_message(
        f"Report `{report_id}` submitted. Moderation has been notified.",
        ephemeral=True
    )

async def modreports(interaction: discord.Interaction, limit: int = 15):
    if not await ensure_moderator(interaction):
        return
    if limit < 1 or limit > 50:
        await interaction.response.send_message("Limit must be between 1 and 50.", ephemeral=True)
        return
    reports = [r for r in get_moderation_state().get("reports", []) if r.get("status") == "open"]
    if not reports:
        await interaction.response.send_message("No open reports.", ephemeral=True)
        return
    lines = []
    for r in reports[-limit:]:
        lines.append(
            f"- {r.get('id')} | accused={r.get('accused_user_id')} | reporter={r.get('reporter_user_id')} | {r.get('reason')}"
        )
    await interaction.response.send_message("Open reports:\n" + "\n".join(lines), ephemeral=True)

async def modwarn(interaction: discord.Interaction, user_id: str, reason: str = "No reason provided"):
    if not await ensure_moderator(interaction):
        return
    parsed = parse_discord_user_id(user_id)
    if not parsed:
        await interaction.response.send_message("Invalid user ID.", ephemeral=True)
        return
    sanctions = get_user_sanctions(parsed)
    sanctions["warnings"] = int(sanctions.get("warnings", 0)) + 1
    sanctions["reason"] = reason
    add_mod_event("warn", target_user_id=parsed, moderator_user_id=interaction.user.id, details=reason)
    save_systems()
    await interaction.response.send_message(
        f"Warned `{parsed}`. Total warnings: {sanctions['warnings']}",
        ephemeral=True
    )

@app_commands.choices(scope=[
    app_commands.Choice(name="External messaging only", value="external"),
    app_commands.Choice(name="All bot commands", value="all"),
])
async def modsuspend(interaction: discord.Interaction, user_id: str, hours: int = 24, scope: str = "external", reason: str = "No reason provided"):
    if not await ensure_moderator(interaction):
        return
    parsed = parse_discord_user_id(user_id)
    if not parsed:
        await interaction.response.send_message("Invalid user ID.", ephemeral=True)
        return
    if hours < 1 or hours > 720:
        await interaction.response.send_message("Hours must be between 1 and 720.", ephemeral=True)
        return
    sanctions = get_user_sanctions(parsed)
    sanctions["suspended_until"] = (datetime.now(timezone.utc) + timedelta(hours=hours)).isoformat()
    sanctions["scope"] = scope
    sanctions["reason"] = reason
    add_mod_event("suspend", target_user_id=parsed, moderator_user_id=interaction.user.id, details=f"scope={scope},hours={hours},reason={reason}")
    save_systems()
    await interaction.response.send_message(f"Suspended `{parsed}` for {hours} hour(s), scope={scope}.", ephemeral=True)

@app_commands.choices(scope=[
    app_commands.Choice(name="External messaging", value="external"),
    app_commands.Choice(name="Full bot", value="all"),
])
async def modban(interaction: discord.Interaction, user_id: str, scope: str = "external", reason: str = "No reason provided"):
    if not await ensure_moderator(interaction):
        return
    parsed = parse_discord_user_id(user_id)
    if not parsed:
        await interaction.response.send_message("Invalid user ID.", ephemeral=True)
        return
    sanctions = get_user_sanctions(parsed)
    if scope == "all":
        sanctions["bot_banned"] = True
    else:
        sanctions["external_banned"] = True
    sanctions["reason"] = reason
    add_mod_event("ban", target_user_id=parsed, moderator_user_id=interaction.user.id, details=f"scope={scope},reason={reason}")
    save_systems()
    await interaction.response.send_message(f"Banned `{parsed}` for scope={scope}.", ephemeral=True)

async def modunban(interaction: discord.Interaction, user_id: str, clear_warnings: bool = False):
    if not await ensure_moderator(interaction):
        return
    parsed = parse_discord_user_id(user_id)
    if not parsed:
        await interaction.response.send_message("Invalid user ID.", ephemeral=True)
        return
    sanctions = get_user_sanctions(parsed)
    sanctions["external_banned"] = False
    sanctions["bot_banned"] = False
    sanctions["suspended_until"] = None
    sanctions["scope"] = "external"
    if clear_warnings:
        sanctions["warnings"] = 0
    add_mod_event("unban", target_user_id=parsed, moderator_user_id=interaction.user.id, details=f"clear_warnings={clear_warnings}")
    save_systems()
    await interaction.response.send_message(f"Cleared bans/suspensions for `{parsed}`.", ephemeral=True)

async def modappeal(interaction: discord.Interaction, message: str):
    user_id = interaction.user.id
    if len(message.strip()) < 5:
        await interaction.response.send_message("Please provide a longer appeal message.", ephemeral=True)
        return
    add_mod_event("appeal", target_user_id=user_id, moderator_user_id=user_id, details=message.strip())
    save_systems()
    await interaction.response.send_message("Your appeal was submitted.", ephemeral=True)

@tree.command(name="blockuser", description="Block a Discord user from sending your system external messages")
async def blockuser(interaction: discord.Interaction, user_id: str):
    owner_id = interaction.user.id
    system_id = get_user_system_id(owner_id)
    if not system_id:
        await interaction.response.send_message("You must register using /register.", ephemeral=True)
        return

    parsed_user_id = parse_discord_user_id(user_id)
    if not parsed_user_id:
        await interaction.response.send_message("Invalid user ID. Use a numeric Discord ID or mention.", ephemeral=True)
        return

    if str(owner_id) == parsed_user_id:
        await interaction.response.send_message("You cannot block yourself.", ephemeral=True)
        return

    system = systems_data["systems"].get(system_id)
    if not system:
        await interaction.response.send_message("System not found.", ephemeral=True)
        return

    settings = get_external_settings(system)
    blocked = settings.get("blocked_users", [])
    if parsed_user_id in blocked:
        await interaction.response.send_message("That user is already blocked.", ephemeral=True)
        return

    blocked.append(parsed_user_id)
    settings["blocked_users"] = blocked
    save_systems()
    await interaction.response.send_message(f"Blocked user ID `{parsed_user_id}`.", ephemeral=True)

@tree.command(name="unblockuser", description="Unblock a Discord user for external messages")
async def unblockuser(interaction: discord.Interaction, user_id: str):
    owner_id = interaction.user.id
    system_id = get_user_system_id(owner_id)
    if not system_id:
        await interaction.response.send_message("You must register using /register.", ephemeral=True)
        return

    parsed_user_id = parse_discord_user_id(user_id)
    if not parsed_user_id:
        await interaction.response.send_message("Invalid user ID. Use a numeric Discord ID or mention.", ephemeral=True)
        return

    system = systems_data["systems"].get(system_id)
    if not system:
        await interaction.response.send_message("System not found.", ephemeral=True)
        return

    settings = get_external_settings(system)
    blocked = settings.get("blocked_users", [])
    if parsed_user_id not in blocked:
        await interaction.response.send_message("That user is not blocked.", ephemeral=True)
        return

    blocked.remove(parsed_user_id)
    settings["blocked_users"] = blocked
    save_systems()
    await interaction.response.send_message(f"Unblocked user ID `{parsed_user_id}`.", ephemeral=True)

async def blockedusers(interaction: discord.Interaction):
    user_id = interaction.user.id
    system_id = get_user_system_id(user_id)
    if not system_id:
        await interaction.response.send_message("You must register using /register.", ephemeral=True)
        return

    system = systems_data["systems"].get(system_id)
    if not system:
        await interaction.response.send_message("System not found.", ephemeral=True)
        return

    blocked = get_external_settings(system).get("blocked_users", [])
    if not blocked:
        await interaction.response.send_message("No blocked users.", ephemeral=True)
        return

    await interaction.response.send_message(
        "Blocked user IDs:\n" + "\n".join([f"- {uid}" for uid in blocked]),
        ephemeral=True
    )

@tree.command(name="sendexternal", description="Send an inbox message to a member in another system")
async def sendexternal(
    interaction: discord.Interaction,
    target_user_id: str,
    target_member_id: str,
    message: str,
    target_subsystem_id: str = None
):
    sender_user_id = interaction.user.id
    sender_system_id = get_user_system_id(sender_user_id)
    if not sender_system_id:
        await interaction.response.send_message("You must register using /register.", ephemeral=True)
        return

    sender_sanctions = get_user_sanctions(sender_user_id)
    if sender_sanctions.get("external_banned"):
        await interaction.response.send_message("You are restricted from external messaging.", ephemeral=True)
        return
    if is_user_suspended(sender_user_id, scope="external"):
        await interaction.response.send_message("You are temporarily suspended from external messaging.", ephemeral=True)
        return

    sender_system = systems_data["systems"].get(sender_system_id)
    sender_ext_settings = get_external_settings(sender_system) if sender_system else None
    if not sender_ext_settings or not sender_ext_settings.get("accept"):
        await interaction.response.send_message(
            "You must enable external messaging first. Use `/allowexternal enabled:True`.",
            ephemeral=True
        )
        return

    parsed_target_user_id = parse_discord_user_id(target_user_id)
    if not parsed_target_user_id:
        await interaction.response.send_message("Invalid target user ID. Use a numeric Discord ID or mention.", ephemeral=True)
        return

    if str(sender_user_id) == parsed_target_user_id:
        await interaction.response.send_message("Use /messageto for your own system.", ephemeral=True)
        return

    if not check_and_update_external_rate_limit(sender_user_id):
        await interaction.response.send_message(
            f"Rate limited. You can send up to {EXTERNAL_MSG_LIMIT_COUNT} external messages per {EXTERNAL_MSG_LIMIT_SECONDS} seconds.",
            ephemeral=True
        )
        return

    target_system_id = get_user_system_id(parsed_target_user_id)
    if not target_system_id:
        await interaction.response.send_message("Target user does not have a registered system.", ephemeral=True)
        return

    target_system = systems_data["systems"].get(target_system_id)
    if not target_system or not sender_system:
        await interaction.response.send_message("System data not found.", ephemeral=True)
        return

    target_settings = get_external_settings(target_system)
    prune_external_temp_blocks(target_settings)
    cleaned = cleanup_external_inbox_entries(target_system)
    if cleaned:
        save_systems()

    if not target_settings.get("accept"):
        await interaction.response.send_message("That system is not accepting external messages.", ephemeral=True)
        return

    if str(sender_user_id) in target_settings.get("blocked_users", []):
        add_external_audit_entry(target_system, "blocked_reject", sender_user_id, details="blocked_users")
        await interaction.response.send_message("You are blocked by that system.", ephemeral=True)
        return

    if str(sender_user_id) in target_settings.get("muted_users", []):
        add_external_audit_entry(target_system, "muted_drop", sender_user_id)
        await interaction.response.send_message("Message not delivered.", ephemeral=True)
        save_systems()
        return

    temp_blocks = target_settings.get("temp_blocks", {})
    until_iso = temp_blocks.get(str(sender_user_id))
    if until_iso:
        add_external_audit_entry(target_system, "tempblock_reject", sender_user_id, details=f"until={until_iso}")
        await interaction.response.send_message("You are temporarily blocked by that system.", ephemeral=True)
        save_systems()
        return

    max_len = int(target_settings.get("message_max_length", 1500))
    if len(message) > max_len:
        await interaction.response.send_message(
            f"Message too long for that recipient. Max length is {max_len} characters.",
            ephemeral=True
        )
        return

    target_rate_seconds = int(target_settings.get("target_rate_seconds", EXTERNAL_TARGET_LIMIT_SECONDS))
    if not check_and_update_external_target_rate_limit(sender_user_id, parsed_target_user_id, target_rate_seconds):
        await interaction.response.send_message(
            f"Slow down. You can send to that recipient once every {target_rate_seconds} seconds.",
            ephemeral=True
        )
        return

    target_members, _, target_member = resolve_target_member_scope(
        target_system,
        target_system_id,
        target_member_id,
        target_subsystem_id
    )
    if target_members is None or target_member is None:
        if target_subsystem_id is not None:
            await interaction.response.send_message("Target member not found in that subsystem.", ephemeral=True)
        else:
            await interaction.response.send_message("Target member not found in that system (main or subsystems).", ephemeral=True)
        return

    sender_mode = sender_system.get("mode", "system")
    if sender_mode == "singlet":
        sender_name = sender_system.get("system_name") or interaction.user.display_name
    else:
        _, sender_member = get_fronting_member_for_user(sender_user_id)
        sender_name = sender_member.get("name", "Unknown") if sender_member else "Unknown"
    sender_system_name = sender_system.get("system_name", f"System {sender_system_id}")

    if target_settings.get("trusted_only") and str(sender_user_id) not in target_settings.get("trusted_users", []):
        target_settings.setdefault("pending_requests", []).append({
            "sender_user_id": str(sender_user_id),
            "from_system_id": str(sender_system_id),
            "from": f"{sender_name} ({sender_system_name})",
            "target_member_id": str(target_member_id),
            "target_subsystem_id": target_subsystem_id,
            "text": message,
            "sent_at": datetime.now(timezone.utc).isoformat(),
        })
        add_external_audit_entry(target_system, "pending_approval", sender_user_id, target_member_id=target_member_id)
        save_systems()
        await interaction.response.send_message(
            "Your message is queued pending recipient approval (trusted-senders-only mode).",
            ephemeral=True
        )
        return

    target_members[target_member_id].setdefault("inbox", [])
    target_members[target_member_id]["inbox"].append({
        "from": f"{sender_name} ({sender_system_name})",
        "from_user_id": str(sender_user_id),
        "from_system_id": str(sender_system_id),
        "text": message,
        "sent_at": datetime.now(timezone.utc).isoformat(),
        "external": True
    })
    add_external_audit_entry(target_system, "delivered", sender_user_id, target_member_id=target_member_id)
    save_systems()

    await interaction.response.send_message(
        f"External message sent to **{target_member['name']}**.",
        ephemeral=True
    )

# -----------------------------
# Switch member
# -----------------------------
@tree.command(name="switchmember", description="Log a member as fronting in a subsystem")
@app_commands.autocomplete(member_id=switchmember_member_id_autocomplete)
@app_commands.autocomplete(subsystem_id=subsystem_id_autocomplete)
async def switchmember(interaction: discord.Interaction, member_id: str, subsystem_id: str = None):
    user_id = interaction.user.id
    system_id = get_user_system_id(user_id)
    if not system_id:
        await interaction.response.send_message("You must register a main system first using /register.", ephemeral=True)
        return
    members = get_system_members(system_id, subsystem_id)
    if members is None:
        await interaction.response.send_message(f"Member not found in {get_scope_label(subsystem_id)}.", ephemeral=True)
        return

    resolved_member_id, resolved_member, resolve_error = resolve_member_identifier(members, member_id)
    if resolve_error:
        await interaction.response.send_message(resolve_error, ephemeral=True)
        return

    # End all currently fronting members first
    for m in members.values():
        if m.get("current_front"):
            end_front(m["id"], members_dict=members)

    # Start fronting the selected member
    start_front(resolved_member_id, members_dict=members)

    # Build response
    new_name = resolved_member.get("name", resolved_member_id)
    response = f"Member **{new_name}** is now fronting in {get_scope_label(subsystem_id)}."

    # Show and clear any pending inbox messages
    system = systems_data.get("systems", {}).get(system_id, {})
    if cleanup_external_inbox_entries(system):
        save_systems()

    inbox = resolved_member.get("inbox", [])
    if inbox:
        external_settings = get_external_settings(system)
        delivery_mode = external_settings.get("delivery_mode", "public")

        external_msgs = [m for m in inbox if m.get("external")]
        internal_msgs = [m for m in inbox if not m.get("external")]

        response += f"\n\n📨 **You have {len(inbox)} message(s):**"

        # Internal messages are always shown in-channel.
        for msg in internal_msgs:
            response += f"\n{format_inbox_entry_for_channel(msg)}"

        # External message visibility depends on privacy mode.
        if external_msgs and delivery_mode == "public":
            for msg in external_msgs:
                response += f"\n{format_inbox_entry_for_channel(msg)}"
        elif external_msgs and delivery_mode == "private_summary":
            response += f"\n> You have **{len(external_msgs)} external** message(s). Details sent by DM."
        elif external_msgs and delivery_mode == "dm_only":
            response += "\n> You have new external messages. Details sent by DM."

        if external_msgs and delivery_mode in {"private_summary", "dm_only"}:
            try:
                if is_in_quiet_hours(system):
                    response += "\n> Quiet hours are active, so external details were not DM'd right now."
                else:
                    dm_lines = [
                        f"External inbox for {resolved_member.get('name', resolved_member_id)} ({get_scope_label(subsystem_id)}):"
                    ]
                    for idx, msg in enumerate(external_msgs, start=1):
                        dm_lines.append(f"\n#{idx}\n{format_inbox_entry_for_dm(msg)}")
                    await interaction.user.send("\n".join(dm_lines))
            except (discord.Forbidden, discord.HTTPException):
                response += "\n> I could not DM you. External message details are hidden due to your privacy mode."

        resolved_member["inbox"] = []

    save_systems()
    await interaction.response.send_message(response)

# -----------------------------
# Co-front member
# -----------------------------
@tree.command(name="cofrontmember", description="Select co-fronting members interactively in a subsystem")
@app_commands.describe(member="Member name or ID", subsystem_id="Subsystem to search (leave blank to search entire system)")
@app_commands.autocomplete(subsystem_id=subsystem_id_autocomplete)
async def cofrontmember(interaction: discord.Interaction, member: str, subsystem_id: str = None):
    user_id = interaction.user.id
    system_id = get_user_system_id(user_id)
    if not system_id:
        await interaction.response.send_message("You must register a main system first using /register.", ephemeral=True)
        return
    system = systems_data.get("systems", {}).get(system_id, {})

    target_scope_id, members, resolved_member_id, resolved_member, resolve_error = \
        resolve_member_identifier_in_system(system, member, subsystem_id=subsystem_id)
    if resolve_error:
        await interaction.response.send_message(resolve_error, ephemeral=True)
        return

    view = CoFrontView(members, resolved_member_id)
    await interaction.response.send_message(
        (
            f"Select co-front members for **{resolved_member.get('name', resolved_member_id)}** in {get_scope_label(target_scope_id)} then click Confirm. "
            f"Page 1/{view.total_pages}. Selected: 0"
        ),
        view=view,
        ephemeral=True
    )
    await view.wait()

    if view.cancelled:
        return

    selected_cofronts = [mid for mid, _ in view.member_items if mid in view.selected_cofronts]

    # End any current fronting sessions
    for m in members.values():
        if m.get("current_front"):
            end_front(m["id"], members_dict=members)

    # Start the front session with co-fronts
    start_front(resolved_member_id, cofronts=selected_cofronts, members_dict=members)
    save_systems()

    co_names = ", ".join([members[c]["name"] for c in selected_cofronts]) if selected_cofronts else "None"
    await interaction.followup.send(
        f"Member **{resolved_member.get('name', resolved_member_id)}** is now fronting with co-fronts: {co_names} in {get_scope_label(target_scope_id)}.",
        ephemeral=True
    )

# -----------------------------
# Autoproxy
# -----------------------------
@tree.command(name="globalautoproxy", description="Set global autoproxy mode: off, front, or latch")
@app_commands.choices(mode=[
    app_commands.Choice(name="Off", value="off"),
    app_commands.Choice(name="Front (current fronter)", value="front"),
    app_commands.Choice(name="Latch (last tagged proxy)", value="latch"),
])
async def globalautoproxy(interaction: discord.Interaction, mode: str):
    user_id = interaction.user.id
    system_id = get_user_system_id(user_id)
    if not system_id:
        await interaction.response.send_message("You must register a main system first using /register.", ephemeral=True)
        return

    system = systems_data["systems"].get(system_id)
    if not system:
        await interaction.response.send_message("System not found.", ephemeral=True)
        return

    settings = get_autoproxy_settings(system)
    apply_autoproxy_mode(settings, mode)
    save_systems()

    if mode == "off":
        message = "Global autoproxy is now **off**."
    elif mode == "front":
        message = "Global autoproxy is now **front**. Your untagged messages will proxy as the current fronter."
    else:
        message = "Global autoproxy is now **latch**. Your untagged messages will proxy as the last member you explicitly proxied with a tag."

    await interaction.response.send_message(message, ephemeral=True)

@tree.command(name="autoproxy", description="Set per-server autoproxy mode: off, front, latch, or inherit global")
@app_commands.choices(mode=[
    app_commands.Choice(name="Inherit Global", value="inherit"),
    app_commands.Choice(name="Off", value="off"),
    app_commands.Choice(name="Front (current fronter)", value="front"),
    app_commands.Choice(name="Latch (last tagged proxy)", value="latch"),
])
async def autoproxy_server(interaction: discord.Interaction, mode: str):
    if interaction.guild_id is None:
        await interaction.response.send_message("This command can only be used in a server.", ephemeral=True)
        return

    user_id = interaction.user.id
    system_id = get_user_system_id(user_id)
    if not system_id:
        await interaction.response.send_message("You must register a main system first using /register.", ephemeral=True)
        return

    system = systems_data["systems"].get(system_id)
    if not system:
        await interaction.response.send_message("System not found.", ephemeral=True)
        return

    overrides = system.setdefault("autoproxy_server_overrides", {})
    guild_key = str(interaction.guild_id)

    if mode == "inherit":
        if guild_key in overrides:
            overrides.pop(guild_key, None)
            save_systems()
        await interaction.response.send_message("Server autoproxy now inherits your global autoproxy setting.", ephemeral=True)
        return

    settings = get_server_autoproxy_settings(system, interaction.guild_id, create=True)
    apply_autoproxy_mode(settings, mode)
    save_systems()

    if mode == "off":
        message = "Server autoproxy is now **off** for this server."
    elif mode == "front":
        message = "Server autoproxy is now **front** for this server."
    else:
        message = "Server autoproxy is now **latch** for this server."

    await interaction.response.send_message(message, ephemeral=True)

@tree.command(name="autoproxystatus", description="View effective global/server autoproxy settings")
async def autoproxystatus(interaction: discord.Interaction):
    user_id = interaction.user.id
    system_id = get_user_system_id(user_id)
    if not system_id:
        await interaction.response.send_message("You must register a main system first using /register.", ephemeral=True)
        return

    system = systems_data["systems"].get(system_id)
    if not system:
        await interaction.response.send_message("System not found.", ephemeral=True)
        return

    global_settings = get_autoproxy_settings(system)
    guild_id = interaction.guild_id
    server_settings = get_server_autoproxy_settings(system, guild_id, create=False) if guild_id else None

    lines = [f"Global mode: **{global_settings.get('mode', 'off')}**"]
    if guild_id is None:
        lines.append("Server mode: N/A (DM context)")
        lines.append(f"Effective mode: **{global_settings.get('mode', 'off')}**")
    elif server_settings is None:
        lines.append("Server mode: **inherit global**")
        lines.append(f"Effective mode: **{global_settings.get('mode', 'off')}**")
    else:
        lines.append(f"Server mode: **{server_settings.get('mode', 'off')}**")
        lines.append(f"Effective mode: **{server_settings.get('mode', 'off')}**")

    await interaction.response.send_message("\n".join(lines), ephemeral=True)

# -----------------------------
# Front reminders
# -----------------------------
@tree.command(name="frontreminders", description="Enable or disable DM reminders for long fronting sessions")
async def frontreminders(interaction: discord.Interaction, enabled: bool):
    user_id = interaction.user.id
    system_id = get_user_system_id(user_id)
    if not system_id:
        await interaction.response.send_message("You must register a main system first using /register.", ephemeral=True)
        return

    system = systems_data["systems"].get(system_id)
    if not system:
        await interaction.response.send_message("System not found.", ephemeral=True)
        return

    settings = get_front_reminder_settings(system)
    settings["enabled"] = enabled
    save_systems()

    status = "enabled" if enabled else "disabled"
    await interaction.response.send_message(
        f"Front reminders are now **{status}**. Threshold: **{settings['hours']}** hour(s).",
        ephemeral=True
    )

@tree.command(name="setfrontreminderhours", description="Set the DM reminder threshold in hours")
async def setfrontreminderhours(interaction: discord.Interaction, hours: int):
    user_id = interaction.user.id
    system_id = get_user_system_id(user_id)
    if not system_id:
        await interaction.response.send_message("You must register a main system first using /register.", ephemeral=True)
        return

    if hours < 1 or hours > 168:
        await interaction.response.send_message("Hours must be between 1 and 168.", ephemeral=True)
        return

    system = systems_data["systems"].get(system_id)
    if not system:
        await interaction.response.send_message("System not found.", ephemeral=True)
        return

    settings = get_front_reminder_settings(system)
    settings["hours"] = hours
    save_systems()

    await interaction.response.send_message(
        f"Front reminder threshold set to **{hours}** hour(s).",
        ephemeral=True
    )

@tree.command(name="frontreminderstatus", description="View your DM front reminder settings")
async def frontreminderstatus(interaction: discord.Interaction):
    user_id = interaction.user.id
    system_id = get_user_system_id(user_id)
    if not system_id:
        await interaction.response.send_message("You must register a main system first using /register.", ephemeral=True)
        return

    system = systems_data["systems"].get(system_id)
    if not system:
        await interaction.response.send_message("System not found.", ephemeral=True)
        return

    settings = get_front_reminder_settings(system)
    enabled_text = "enabled" if settings.get("enabled") else "disabled"
    await interaction.response.send_message(
        f"Front reminders are **{enabled_text}**. Threshold: **{settings['hours']}** hour(s). Sent by DM once per front session.",
        ephemeral=True
    )

# -----------------------------
# Check-ins
# -----------------------------
@tree.command(name="checkin", description="Log a system check-in with mood rating and energy")
@app_commands.choices(energy=[
    app_commands.Choice(name="Very Low", value="very_low"),
    app_commands.Choice(name="Low", value="low"),
    app_commands.Choice(name="Medium", value="medium"),
    app_commands.Choice(name="High", value="high"),
    app_commands.Choice(name="Very High", value="very_high"),
])
async def checkin(interaction: discord.Interaction, rating: int, energy: str, notes: str = None):
    user_id = interaction.user.id
    system_id = get_user_system_id(user_id)
    if not system_id:
        await interaction.response.send_message("You must register a main system first using /register.", ephemeral=True)
        return

    if rating < 1 or rating > 10:
        await interaction.response.send_message("Rating must be between 1 and 10.", ephemeral=True)
        return

    system = systems_data["systems"].get(system_id)
    if not system:
        await interaction.response.send_message("System not found.", ephemeral=True)
        return

    _, front_member = get_fronting_member_for_user(user_id)
    entry = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "rating": rating,
        "energy": energy,
        "notes": notes,
        "front_member_id": front_member.get("id") if front_member else None,
        "front_member_name": front_member.get("name") if front_member else None,
    }

    settings = get_checkin_settings(system)
    settings.setdefault("entries", []).append(entry)
    # Keep only the most recent 500 entries per system.
    if len(settings["entries"]) > 500:
        settings["entries"] = settings["entries"][-500:]

    save_systems()

    trend_text = get_checkin_trend_text(system)
    energy_text = energy.replace("_", " ").title()
    front_text = front_member.get("name") if front_member else "No active fronter"
    await interaction.response.send_message(
        f"Check-in logged: **{rating}/10**, energy **{energy_text}**.\n"
        f"Current front: **{front_text}**\n"
        f"{trend_text}",
        ephemeral=True
    )

@tree.command(name="weeklymoodsummary", description="Enable or disable weekly mood summary DMs")
async def weeklymoodsummary(interaction: discord.Interaction, enabled: bool):
    user_id = interaction.user.id
    system_id = get_user_system_id(user_id)
    if not system_id:
        await interaction.response.send_message("You must register a main system first using /register.", ephemeral=True)
        return

    system = systems_data["systems"].get(system_id)
    if not system:
        await interaction.response.send_message("System not found.", ephemeral=True)
        return

    settings = get_checkin_settings(system)
    settings["weekly_dm_enabled"] = enabled
    save_systems()

    status = "enabled" if enabled else "disabled"
    await interaction.response.send_message(f"Weekly mood summary DMs are now **{status}**.", ephemeral=True)

@tree.command(name="checkinstatus", description="View your current check-in settings and trend")
async def checkinstatus(interaction: discord.Interaction):
    user_id = interaction.user.id
    system_id = get_user_system_id(user_id)
    if not system_id:
        await interaction.response.send_message("You must register a main system first using /register.", ephemeral=True)
        return

    system = systems_data["systems"].get(system_id)
    if not system:
        await interaction.response.send_message("System not found.", ephemeral=True)
        return

    settings = get_checkin_settings(system)
    trend_text = get_checkin_trend_text(system)
    weekly_status = "enabled" if settings.get("weekly_dm_enabled", True) else "disabled"
    await interaction.response.send_message(
        f"Weekly mood summaries: **{weekly_status}**\n"
        f"Recent check-ins (7d): **{len(_recent_checkins(system, 7))}**\n"
        f"{trend_text}",
        ephemeral=True
    )

# -----------------------------
# Focus Mode slash commands (singlet only)
# -----------------------------

async def mode_name_autocomplete(interaction: discord.Interaction, current: str):
    system_id = get_user_system_id(interaction.user.id)
    if not system_id:
        return []
    system = systems_data["systems"].get(system_id)
    if not system:
        return []
    choices = get_all_mode_names(system)
    filtered = [c for c in choices if current.lower() in c.lower()]
    return [app_commands.Choice(name=c.title(), value=c) for c in filtered[:25]]

@tree.command(name="setmode", description="Set your current focus mode (singlet only)")
@app_commands.autocomplete(mode_name=mode_name_autocomplete)
async def setmode(
    interaction: discord.Interaction,
    mode_name: str = None,
    clear: bool = False,
):
    user_id = interaction.user.id
    system_id = get_user_system_id(user_id)
    if not system_id:
        await interaction.response.send_message("You must register a profile first using /register.", ephemeral=True)
        return
    system = systems_data["systems"].get(system_id)
    if not system:
        await interaction.response.send_message("System not found.", ephemeral=True)
        return
    if system.get("mode") != "singlet":
        await interaction.response.send_message("Focus modes are only available for singlet profiles.", ephemeral=True)
        return

    if clear:
        ended = end_focus_mode(system)
        save_systems()
        if ended:
            await interaction.response.send_message("Focus mode cleared.", ephemeral=True)
        else:
            await interaction.response.send_message("No active focus mode to clear.", ephemeral=True)
        return

    if not mode_name:
        await interaction.response.send_message(
            "Provide a mode name or use `clear: True` to end the current mode.\n"
            f"Default modes: {', '.join(m.title() for m in DEFAULT_FOCUS_MODES)}",
            ephemeral=True,
        )
        return

    mode_clean = mode_name.strip().lower()
    if not mode_clean:
        await interaction.response.send_message("Mode name cannot be empty.", ephemeral=True)
        return

    all_modes = get_all_mode_names(system)
    fm = get_focus_modes(system)
    if mode_clean not in all_modes:
        fm.setdefault("custom_modes", []).append(mode_clean)

    start_focus_mode(system, mode_clean)
    save_systems()
    await interaction.response.send_message(
        f"Focus mode set to **{mode_clean.title()}**.", ephemeral=True
    )

@tree.command(name="currentmode", description="View your current focus mode (singlet only)")
async def currentmode(interaction: discord.Interaction):
    user_id = interaction.user.id
    system_id = get_user_system_id(user_id)
    if not system_id:
        await interaction.response.send_message("You must register a profile first using /register.", ephemeral=True)
        return
    system = systems_data["systems"].get(system_id)
    if not system:
        await interaction.response.send_message("System not found.", ephemeral=True)
        return
    if system.get("mode") != "singlet":
        await interaction.response.send_message("Focus modes are only available for singlet profiles.", ephemeral=True)
        return

    fm = get_focus_modes(system)
    cur = fm.get("current")
    if not cur:
        await interaction.response.send_message(
            "No active focus mode. Use `/setmode` to set one.", ephemeral=True
        )
        return

    started_dt = datetime.fromisoformat(cur["started"])
    elapsed = (datetime.now(timezone.utc) - started_dt).total_seconds()
    dur = format_duration(elapsed)
    await interaction.response.send_message(
        f"Current mode: **{cur['name'].title()}**\nActive for: **{dur}**",
        ephemeral=True,
    )

@tree.command(name="modestats", description="View time spent in each focus mode (singlet only)")
async def modestats(interaction: discord.Interaction, days: int = 30):
    user_id = interaction.user.id
    system_id = get_user_system_id(user_id)
    if not system_id:
        await interaction.response.send_message("You must register a profile first using /register.", ephemeral=True)
        return
    system = systems_data["systems"].get(system_id)
    if not system:
        await interaction.response.send_message("System not found.", ephemeral=True)
        return
    if system.get("mode") != "singlet":
        await interaction.response.send_message("Focus modes are only available for singlet profiles.", ephemeral=True)
        return

    if days < 1 or days > 365:
        await interaction.response.send_message("Days must be between 1 and 365.", ephemeral=True)
        return

    totals = calc_mode_stats(system, days=days)
    if not totals:
        await interaction.response.send_message(
            f"No focus mode data for the last **{days}** day(s). Use `/setmode` to start tracking.",
            ephemeral=True,
        )
        return

    sorted_modes = sorted(totals.items(), key=lambda x: x[1], reverse=True)
    total_all = sum(totals.values())
    lines = []
    for name, secs in sorted_modes:
        pct = (secs / total_all * 100) if total_all else 0
        lines.append(f"• **{name.title()}** — {format_duration(secs)} ({pct:.0f}%)")

    embed = discord.Embed(
        title=f"Focus Mode Stats — Last {days} Day(s)",
        description="\n".join(lines),
        color=discord.Color.blurple(),
    )
    embed.set_footer(text=f"Total tracked: {format_duration(total_all)}")
    await interaction.response.send_message(embed=embed, ephemeral=True)

@tree.command(name="settimezone", description="Set your system timezone (IANA, e.g. America/New_York)")
@app_commands.autocomplete(timezone_name=timezone_autocomplete)
async def settimezone(interaction: discord.Interaction, timezone_name: str):
    user_id = interaction.user.id
    system_id = get_user_system_id(user_id)
    if not system_id:
        await interaction.response.send_message("You must register a main system first using /register.", ephemeral=True)
        return

    normalized = normalize_timezone_name(timezone_name)
    if not normalized:
        await interaction.response.send_message("Please provide a timezone.", ephemeral=True)
        return

    # Validate against ZoneInfo when available; fallback to known timezone lists on systems without tzdata.
    valid = False
    try:
        ZoneInfo(normalized)
        valid = True
    except ZoneInfoNotFoundError:
        if normalized in TIMEZONE_FIXED_OFFSETS or normalized in COMMON_TIMEZONES:
            valid = True

    if not valid:
        await interaction.response.send_message(
            "Unknown timezone. Pick from the dropdown, or use values like America/New_York, Europe/London, Asia/Tokyo, or UTC.",
            ephemeral=True
        )
        return

    system = systems_data["systems"].get(system_id)
    if not system:
        await interaction.response.send_message("System not found.", ephemeral=True)
        return

    system["timezone"] = normalized
    save_systems()
    await interaction.response.send_message(f"Timezone set to **{normalized}**.", ephemeral=True)

@tree.command(name="timezonestatus", description="View your current system timezone")
async def timezonestatus(interaction: discord.Interaction, show_to_others: bool = False):
    user_id = interaction.user.id
    system_id = get_user_system_id(user_id)
    if not system_id:
        await interaction.response.send_message("You must register a main system first using /register.", ephemeral=True)
        return

    system = systems_data["systems"].get(system_id)
    if not system:
        await interaction.response.send_message("System not found.", ephemeral=True)
        return

    await interaction.response.send_message(
        f"Current timezone: **{get_system_timezone_name(system)}**",
        ephemeral=not show_to_others
    )

@tree.command(name="setstatus", description="Set or clear a front status for a currently fronting member")
@app_commands.autocomplete(member_id=switchmember_member_id_autocomplete)
@app_commands.autocomplete(subsystem_id=subsystem_id_autocomplete)
async def setstatus(
    interaction: discord.Interaction,
    status: str = None,
    member_id: str = None,
    subsystem_id: str = None,
    clear: bool = False,
):
    user_id = interaction.user.id
    system_id = get_user_system_id(user_id)
    if not system_id:
        await interaction.response.send_message("You must register a main system first using /register.", ephemeral=True)
        return

    members_dict = get_system_members(system_id, subsystem_id)
    if members_dict is None:
        await interaction.response.send_message("Subsystem not found.", ephemeral=True)
        return

    if clear:
        cleaned_status = None
    else:
        cleaned_status = (status or "").strip()
        if not cleaned_status:
            await interaction.response.send_message(
                "Provide a status, or use `clear:true` to remove it.",
                ephemeral=True
            )
            return

    target_member = None
    if member_id:
        if member_id not in members_dict:
            await interaction.response.send_message(f"Member not found in {get_scope_label(subsystem_id)}.", ephemeral=True)
            return
        target_member = members_dict[member_id]
    else:
        active = [m for m in members_dict.values() if m.get("current_front")]
        if not active:
            await interaction.response.send_message(
                f"No one is currently fronting in {get_scope_label(subsystem_id)}.",
                ephemeral=True
            )
            return
        if len(active) > 1:
            await interaction.response.send_message(
                "Multiple members are currently fronting. Please specify `member_id`.",
                ephemeral=True
            )
            return
        target_member = active[0]

    if not target_member.get("current_front"):
        await interaction.response.send_message(
            f"**{target_member['name']}** is not currently fronting in {get_scope_label(subsystem_id)}.",
            ephemeral=True
        )
        return

    target_member["current_front"]["status"] = cleaned_status
    save_systems()

    if cleaned_status is None:
        await interaction.response.send_message(
            f"Cleared front status for **{target_member['name']}**.",
            ephemeral=True
        )
    else:
        await interaction.response.send_message(
            f"Set front status for **{target_member['name']}** to: {cleaned_status}",
            ephemeral=True
        )

# -----------------------------
# Current fronts
# -----------------------------
@tree.command(name="currentfronts", description="View current fronting members in a subsystem")
@app_commands.autocomplete(subsystem_id=subsystem_id_autocomplete)
async def currentfronts(interaction: discord.Interaction, subsystem_id: str = None):
    user_id = interaction.user.id
    system_id = get_user_system_id(user_id)
    if not system_id:
        await interaction.response.send_message("You must register a main system first using /register.", ephemeral=True)
        return
    system = systems_data.get("systems", {}).get(system_id)
    if not system:
        await interaction.response.send_message("System not found.", ephemeral=True)
        return

    if subsystem_id is None:
        scope_sets = list(iter_system_member_dicts(system))
        title = "Current Fronts - Entire System"
    else:
        members_dict = get_system_members(system_id, subsystem_id)
        if members_dict is None:
            await interaction.response.send_message("Subsystem not found.", ephemeral=True)
            return
        scope_sets = [(subsystem_id, members_dict)]
        title = f"Current Fronts - {get_scope_label(subsystem_id).capitalize()}"

    fronters = []
    for scope_id, scoped_members in scope_sets:
        scope_label = get_scope_label(scope_id)
        for m in scoped_members.values():
            current = m.get("current_front")
            if not current:
                continue

            start_dt = datetime.fromisoformat(current["start"])
            duration = format_duration((datetime.now(timezone.utc) - start_dt).total_seconds())
            cofront_names = [scoped_members[c]["name"] for c in current.get("cofronts", []) if c in scoped_members]
            cofront_str = f" — Co-fronting: {', '.join(cofront_names)}" if cofront_names else ""
            status_text = (current.get("status") or "").strip()
            status_str = f" — Status: {status_text}" if status_text else ""
            fronters.append(f"• {m['name']} ({scope_label}) — {duration}{status_str}{cofront_str}")

    embed = discord.Embed(
        title=title,
        description="\n".join(fronters) if fronters else "No members are currently fronting.",
        color=discord.Color.purple()
    )

    await interaction.response.send_message(embed=embed)

# -----------------------------
# System front history
# -----------------------------
# -----------------------------
# Front history
# -----------------------------
@tree.command(name="fronthistory", description="View recent front history for a subsystem")
@app_commands.autocomplete(subsystem_id=subsystem_id_autocomplete)
async def fronthistory(interaction: discord.Interaction, subsystem_id: str = None):
    user_id = interaction.user.id
    system_id = get_user_system_id(user_id)
    if not system_id:
        await interaction.response.send_message("You must register a main system first using /register.", ephemeral=True)
        return
    members_dict = get_system_members(system_id, subsystem_id)
    if members_dict is None:
        await interaction.response.send_message("Subsystem not found.", ephemeral=True)
        return

    history_entries = []

    # Collect all front sessions
    for m in members_dict.values():
        for entry in m.get("front_history", []):
            start = entry.get("start")
            end = entry.get("end")

            history_entries.append({
                "name": m["name"],
                "start": start,
                "end": end
            })

    if not history_entries:
        await interaction.response.send_message("No front history recorded yet.")
        return

    # Sort by most recent start time
    history_entries.sort(
        key=lambda x: datetime.fromisoformat(x["start"]),
        reverse=True
    )

    page_size = 10
    total_pages = (len(history_entries) - 1) // page_size + 1

    def get_embed(page):

        start_index = page * page_size
        end_index = start_index + page_size
        chunk = history_entries[start_index:end_index]

        lines = []

        for entry in chunk:

            start_time = format_us(entry["start"])

            if entry["end"]:
                end_time = format_us(entry["end"])
                lines.append(
                    f"**{entry['name']}** — {start_time} → {end_time}"
                )
            else:
                lines.append(
                    f"**{entry['name']}** — {start_time} → *Currently fronting*"
                )

        embed = discord.Embed(
            title=f"Front History - {get_scope_label(subsystem_id).capitalize()}",
            description="\n".join(lines),
            color=discord.Color.blue()
        )

        embed.set_footer(text=f"Page {page+1}/{total_pages}")

        return embed

    class HistoryView(discord.ui.View):

        def __init__(self):
            super().__init__(timeout=120)
            self.page = 0

        @discord.ui.button(label="Previous", style=discord.ButtonStyle.secondary)
        async def previous(self, interaction: discord.Interaction, button: discord.ui.Button):

            if self.page > 0:
                self.page -= 1
                await interaction.response.edit_message(
                    embed=get_embed(self.page),
                    view=self
                )

        @discord.ui.button(label="Next", style=discord.ButtonStyle.secondary)
        async def next(self, interaction: discord.Interaction, button: discord.ui.Button):

            if self.page < total_pages - 1:
                self.page += 1
                await interaction.response.edit_message(
                    embed=get_embed(self.page),
                    view=self
                )

    view = HistoryView()

    await interaction.response.send_message(
        embed=get_embed(0),
        view=view
    )
# -----------------------------
# Front duration statistics
# -----------------------------
@tree.command(name="frontstats", description="Show front duration statistics for members in a subsystem")
@app_commands.autocomplete(subsystem_id=subsystem_id_autocomplete)
async def frontstats(interaction: discord.Interaction, subsystem_id: str = None):
    user_id = interaction.user.id
    system_id = get_user_system_id(user_id)
    if not system_id:
        await interaction.response.send_message("You must register a main system first using /register.", ephemeral=True)
        return
    members_dict = get_system_members(system_id, subsystem_id)
    if members_dict is None:
        await interaction.response.send_message("Subsystem not found.", ephemeral=True)
        return

    stats = []
    for m in members_dict.values():
        total_seconds = calculate_front_duration(m)
        stats.append((m["name"], total_seconds))

    if not stats:
        await interaction.response.send_message("No fronting data yet.")
        return

    # Sort by total front time
    stats.sort(key=lambda x: x[1], reverse=True)
    grand_total = sum(s for _, s in stats)

    page_size = 10
    total_pages = (len(stats) - 1) // page_size + 1

    def get_embed(page):

        start = page * page_size
        end = start + page_size
        chunk = stats[start:end]

        lines = []
        for name, seconds in chunk:
            pct = (seconds / grand_total * 100) if grand_total > 0 else 0
            lines.append(f"**{name}** — {format_duration(seconds)} ({pct:.1f}%)")

        embed = discord.Embed(
            title=f"Front Duration Statistics - {get_scope_label(subsystem_id).capitalize()}",
            description="\n".join(lines),
            color=discord.Color.orange()
        )

        embed.set_footer(text=f"Page {page+1}/{total_pages}")

        return embed

    class StatsView(discord.ui.View):
        def __init__(self):
            super().__init__(timeout=120)
            self.page = 0

        @discord.ui.button(label="Previous", style=discord.ButtonStyle.secondary)
        async def previous(self, interaction: discord.Interaction, button: discord.ui.Button):
            if self.page > 0:
                self.page -= 1
                await interaction.response.edit_message(embed=get_embed(self.page), view=self)

        @discord.ui.button(label="Next", style=discord.ButtonStyle.secondary)
        async def next(self, interaction: discord.Interaction, button: discord.ui.Button):
            if self.page < total_pages - 1:
                self.page += 1
                await interaction.response.edit_message(embed=get_embed(self.page), view=self)

    view = StatsView()

    await interaction.response.send_message(embed=get_embed(0), view=view)

@tree.command(name="switchpatterns", description="Show switch pair, co-front, and time-of-day fronting patterns")
async def switchpatterns(interaction: discord.Interaction):
    user_id = interaction.user.id
    system_id = get_user_system_id(user_id)
    if not system_id:
        await interaction.response.send_message("You must register a main system first using /register.", ephemeral=True)
        return

    system = systems_data.get("systems", {}).get(system_id)
    if not system:
        await interaction.response.send_message("System not found.", ephemeral=True)
        return

    system_tz = get_system_timezone(system)
    tz_label = get_system_timezone_name(system)

    sessions = []
    members_by_scope = {}
    for scope_id, members_dict in iter_system_member_dicts(system):
        members_by_scope[scope_id] = {str(mid): m.get("name", f"ID {mid}") for mid, m in members_dict.items()}
        for member in members_dict.values():
            member_name = member.get("name", "Unknown")
            member_id = str(member.get("id", ""))
            for entry in member.get("front_history", []):
                start_iso = entry.get("start")
                if not start_iso:
                    continue
                try:
                    start_dt = datetime.fromisoformat(start_iso)
                except ValueError:
                    continue
                local_start_dt = start_dt.astimezone(system_tz)
                sessions.append({
                    "start": local_start_dt,
                    "scope_id": scope_id,
                    "member_id": member_id,
                    "member_name": member_name,
                    "cofronts": [str(c) for c in entry.get("cofronts", [])],
                })

    if not sessions:
        await interaction.response.send_message("Not enough front history yet to analyze patterns.", ephemeral=True)
        return

    sessions.sort(key=lambda s: s["start"])

    switch_counts = {}
    for i in range(1, len(sessions)):
        prev_s = sessions[i - 1]
        curr_s = sessions[i]
        if prev_s["scope_id"] == curr_s["scope_id"] and prev_s["member_id"] == curr_s["member_id"]:
            continue
        key = (prev_s["member_name"], curr_s["member_name"])
        switch_counts[key] = switch_counts.get(key, 0) + 1

    cofront_counts = {}
    member_tod = {}
    for s in sessions:
        name = s["member_name"]
        bucket = time_of_day_bucket(s["start"].hour)
        if name not in member_tod:
            member_tod[name] = {"Morning": 0, "Afternoon": 0, "Evening": 0, "Night": 0}
        member_tod[name][bucket] += 1

        scope_names = members_by_scope.get(s["scope_id"], {})
        for co_id in s["cofronts"]:
            co_name = scope_names.get(co_id, f"ID {co_id}")
            if co_name == name:
                continue
            pair = tuple(sorted([name, co_name]))
            cofront_counts[pair] = cofront_counts.get(pair, 0) + 1

    top_switches = sorted(switch_counts.items(), key=lambda x: x[1], reverse=True)[:6]
    top_cofronts = sorted(cofront_counts.items(), key=lambda x: x[1], reverse=True)[:6]

    member_dominance = []
    for member_name, counts in member_tod.items():
        total = sum(counts.values())
        if total <= 0:
            continue
        top_bucket = max(counts, key=counts.get)
        top_count = counts[top_bucket]
        pct = (top_count / total) * 100
        member_dominance.append((member_name, top_bucket, top_count, total, pct))
    member_dominance.sort(key=lambda x: (x[4], x[2]), reverse=True)
    top_time_patterns = member_dominance[:6]

    switch_lines = [f"• **{a} -> {b}** ({count})" for (a, b), count in top_switches]
    cofront_lines = [f"• **{a} + {b}** ({count})" for (a, b), count in top_cofronts]
    time_lines = [
        f"• **{name}**: {bucket} ({count}/{total} starts, {pct:.0f}%)"
        for name, bucket, count, total, pct in top_time_patterns
    ]

    embed = discord.Embed(
        title="Switch Patterns",
        description=f"Based on recorded front history across your main system and subsystems (time buckets use {tz_label}).",
        color=discord.Color.teal()
    )
    embed.add_field(
        name="Most Common Switch Pairs",
        value="\n".join(switch_lines) if switch_lines else "No switch pair data yet.",
        inline=False
    )
    embed.add_field(
        name="Most Common Co-Front Pairs",
        value="\n".join(cofront_lines) if cofront_lines else "No co-front pair data yet.",
        inline=False
    )
    embed.add_field(
        name="Time-of-Day Patterns",
        value="\n".join(time_lines) if time_lines else "No time-of-day pattern data yet.",
        inline=False
    )

    await interaction.response.send_message(embed=embed)
# -----------------------------
# Member statistics
# -----------------------------
@tree.command(name="memberstats", description="View fronting statistics for a member")
@app_commands.autocomplete(subsystem_id=subsystem_id_autocomplete)
async def memberstats(interaction: discord.Interaction, member_id: str, subsystem_id: str = None):
    user_id = interaction.user.id
    system_id = get_user_system_id(user_id)
    if not system_id:
        await interaction.response.send_message("You must register using /register.", ephemeral=True)
        return
    
    members_dict = get_system_members(system_id, subsystem_id)
    if members_dict is None:
        await interaction.response.send_message("Subsystem not found.", ephemeral=True)
        return
    
    if member_id not in members_dict:
        await interaction.response.send_message(f"Member not found in {get_scope_label(subsystem_id)}.", ephemeral=True)
        return

    member = members_dict[member_id]

    primary = member.get("primary_front_time", 0)
    cofront = member.get("cofront_time", 0)
    total = primary + cofront

    sessions = len(member.get("front_history", []))

    avg = primary / sessions if sessions else 0

    longest = 0
    last_front = None

    for entry in member.get("front_history", []):
        start = entry.get("start")
        end = entry.get("end")

        if start and end:
            start_dt = datetime.fromisoformat(start)
            end_dt = datetime.fromisoformat(end)

            duration = (end_dt - start_dt).total_seconds()

            if duration > longest:
                longest = duration

        if start:
            if not last_front or start > last_front:
                last_front = start

    embed = discord.Embed(
        title=f"Member Stats — {member['name']}",
        color=int(member.get("color", "FFFFFF"), 16)
    )

    embed.add_field(
        name="Front Time",
        value=(
            f"Primary: {format_duration(primary)}\n"
            f"Co-Front: {format_duration(cofront)}\n"
            f"Total Presence: {format_duration(total)}"
        ),
        inline=False
    )

    embed.add_field(
        name="Sessions",
        value=(
            f"Total Fronts: {sessions}\n"
            f"Average Session: {format_duration(avg)}\n"
            f"Longest Session: {format_duration(longest)}"
        ),
        inline=False
    )

    if last_front:
        embed.add_field(
            name="Last Front",
            value=format_us(last_front),
            inline=False
        )

    await interaction.response.send_message(embed=embed)
# -----------------------------
# Member front history
# -----------------------------
@tree.command(name="memberhistory", description="View a member's front history")
@app_commands.autocomplete(subsystem_id=subsystem_id_autocomplete)
async def memberhistory(interaction: discord.Interaction, member_id: str, subsystem_id: str = None):
    await interaction.response.defer()

    user_id = interaction.user.id
    system_id = get_user_system_id(user_id)
    if not system_id:
        await interaction.followup.send("You must register using /register.")
        return
    
    members_dict = get_system_members(system_id, subsystem_id)
    if members_dict is None:
        await interaction.followup.send("Subsystem not found.")
        return
    
    if member_id not in members_dict:
        await interaction.followup.send(f"Member not found in {get_scope_label(subsystem_id)}.")
        return

    member = members_dict[member_id]
    history = member.get("front_history", [])
    if not history:
        await interaction.followup.send("No front history for this member.")
        return

    lines = []
    for entry in history[-20:]:
        start_iso = entry.get("start")
        end_iso = entry.get("end")
        start_str = format_us(start_iso) if start_iso else "Unknown"
        end_str = format_us(end_iso) if end_iso else "Current"

        # Duration
        start_dt = datetime.fromisoformat(start_iso)
        end_dt = datetime.fromisoformat(end_iso) if end_iso else datetime.now(timezone.utc)
        total_sec = int((end_dt - start_dt).total_seconds())
        hours, remainder = divmod(total_sec, 3600)
        minutes, seconds = divmod(remainder, 60)
        duration_str = f"{hours}h {minutes}m {seconds}s" if hours else f"{minutes}m {seconds}s"

        cofront_names = [members_dict[c]["name"] for c in entry.get("cofronts", []) if c in members_dict]
        cofront_str = f" — Co-fronts: {', '.join(cofront_names)}" if cofront_names else ""

        lines.append(f"Start: {start_str}\nEnd: {end_str}\nDuration: {duration_str}{cofront_str}")

    embed = discord.Embed(
        title=f"{member['name']} Front History",
        description="\n\n".join(lines),
        color=discord.Color.blue()
    )

    await interaction.followup.send(embed=embed)
# -----------------------------
# Browse members by tags interactively
# -----------------------------

@tree.command(name="browsetags", description="Browse members by tags interactively in a subsystem")
@app_commands.autocomplete(subsystem_id=subsystem_id_autocomplete)
async def browsetags(interaction: discord.Interaction, subsystem_id: str = None):
    user_id = interaction.user.id
    system_id = get_user_system_id(user_id)
    if not system_id:
        await interaction.response.send_message("You must register a main system first using /register.", ephemeral=True)
        return
    members_dict = get_system_members(system_id, subsystem_id)
    if members_dict is None:
        await interaction.response.send_message("Subsystem not found.", ephemeral=True)
        return
    if not PRESET_TAGS:
        await interaction.response.send_message("No tags exist yet.", ephemeral=True)
        return
    embed = discord.Embed(title="Tag Browser", description="Select one or more tags from the dropdown.", color=discord.Color.blue())
    await interaction.response.send_message(embed=embed, view=TagMultiView())
# -----------------------------
# View member
# -----------------------------
@tree.command(name="viewmember", description="View a member profile in a subsystem")
@app_commands.autocomplete(subsystem_id=subsystem_id_autocomplete)
async def viewmember(
    interaction: discord.Interaction,
    member_id: str,
    subsystem_id: str = None,
    target_user_id: str = None,
):
    requester_id = interaction.user.id
    system_id, system, target_owner_id, error = resolve_target_system_for_view(requester_id, target_user_id)
    if error:
        await interaction.response.send_message(error, ephemeral=True)
        return

    members_dict = get_system_members(system_id, subsystem_id)
    if members_dict is None or member_id not in members_dict:
        await interaction.response.send_message(f"Member not found in {get_scope_label(subsystem_id)}.", ephemeral=True)
        return

    member = members_dict[member_id]
    if str(target_owner_id) != str(requester_id) and not can_view_member_data(system, member, requester_id):
        await interaction.response.send_message("You do not have permission to view this member card.", ephemeral=True)
        return

    await interaction.response.send_message(embed=build_member_profile_embed(member, system=system))

@tree.command(name="random", description="View a random member from your full system")
async def randommember(interaction: discord.Interaction, target_user_id: str = None):
    requester_id = interaction.user.id
    system_id, system, target_owner_id, error = resolve_target_system_for_view(requester_id, target_user_id)
    if error:
        await interaction.response.send_message(error, ephemeral=True)
        return

    candidates = []
    for scope_id, members_dict in iter_system_member_dicts(system):
        for member in members_dict.values():
            if str(target_owner_id) != str(requester_id) and not can_view_member_data(system, member, requester_id):
                continue
            candidates.append((scope_id, member))

    if not candidates:
        if str(target_owner_id) == str(requester_id):
            await interaction.response.send_message("You do not have any members yet.", ephemeral=True)
        else:
            await interaction.response.send_message("No visible members were found for that system.", ephemeral=True)
        return

    scope_id, member = random.choice(candidates)
    scope_label = get_scope_label(scope_id)
    await interaction.response.send_message(
        content=f"Random member from {scope_label}:",
        embed=build_member_profile_embed(member, system=system)
    )
# -----------------------------
# Edit member
# -----------------------------
@tree.command(name="editmember", description="Edit member info in a subsystem")
@app_commands.autocomplete(subsystem_id=subsystem_id_autocomplete)
@app_commands.choices(proxy_tag_position=[
    app_commands.Choice(name="Prefix", value="prefix"),
    app_commands.Choice(name="Suffix", value="suffix"),
])
async def editmember(
    interaction: discord.Interaction,
    member_id: str,
    subsystem_id: str = None,
    name: str = None,
    displayname: str = None,
    pronouns: str = None,
    birthday: str = None,
    description: str = None,
    yt_playlist: str = None,
    color: str = None,
    profile_pic: discord.Attachment = None,
    banner: discord.Attachment = None,
    proxy_tag: str = None,
    proxy_tag_position: str = "prefix",
    clear_proxy_tag: bool = False,
    edit_tags: bool = False
):
    user_id = interaction.user.id
    system_id = get_user_system_id(user_id)
    if not system_id:
        await interaction.response.send_message("You must register using /register.", ephemeral=True)
        return
    
    members_dict = get_system_members(system_id, subsystem_id)
    if members_dict is None:
        await interaction.response.send_message("Subsystem not found.", ephemeral=True)
        return
    
    if member_id not in members_dict:
        await interaction.response.send_message(f"Member not found in {get_scope_label(subsystem_id)}.", ephemeral=True)
        return

    member = members_dict[member_id]
    updated_fields = []

    if name:
        member["name"] = name
        updated_fields.append("name")
    if displayname is not None:
        member["display_name"] = displayname.strip() or None
        updated_fields.append("display name")
    if pronouns:
        member["pronouns"] = pronouns
        updated_fields.append("pronouns")
    if birthday:
        member["birthday"] = birthday
        updated_fields.append("birthday")
    if description:
        member["description"] = description
        updated_fields.append("description")
    if yt_playlist:
        member["yt_playlist"] = yt_playlist
        updated_fields.append("playlist")
    if color:
        try:
            member["color"] = normalize_hex(color)
            updated_fields.append("color")
        except:
            await interaction.response.send_message("Invalid HEX color.", ephemeral=True)
            return
    if profile_pic:
        member["profile_pic"] = profile_pic.url
        updated_fields.append("profile picture")
    if banner:
        member["banner"] = banner.url
        updated_fields.append("banner")
    if clear_proxy_tag:
        set_member_proxy_formats(member, [])
        updated_fields.append("proxy tag cleared")
    elif proxy_tag is not None:
        cleaned = proxy_tag.strip()
        if not cleaned:
            await interaction.response.send_message("Proxy tag cannot be empty.", ephemeral=True)
            return
        position = "suffix" if str(proxy_tag_position).lower() == "suffix" else "prefix"
        if position == "suffix":
            set_member_proxy_formats(member, [{"prefix": None, "suffix": cleaned}])
        else:
            set_member_proxy_formats(member, [{"prefix": cleaned, "suffix": None}])
        updated_fields.append(f"proxy tag set to {render_member_proxy_result(member)}")

    if edit_tags:
        view = TagView(preselected=member.get("tags", []))
        await interaction.response.send_message("Select tags then press Confirm.", view=view)
        timed_out = await view.wait()
        if not timed_out:
            member["tags"] = view.selected_tags
            save_systems()
            updated_fields.append("tags")
            summary = ", ".join(updated_fields) if updated_fields else "no fields"
            await interaction.followup.send(f"Member **{member['name']}** updated: {summary}.")
        else:
            save_systems()
            summary = ", ".join(updated_fields) if updated_fields else "no fields"
            await interaction.followup.send(f"Tag selection timed out. Other changes were still saved: {summary}.")
    else:
        save_systems()
        summary = ", ".join(updated_fields) if updated_fields else "no fields"
        await interaction.response.send_message(f"Member **{member['name']}** updated: {summary}.")


@tree.command(name="addmembertag", description="Add an extra proxy format to a member")
@app_commands.autocomplete(subsystem_id=subsystem_id_autocomplete)
async def addmembertag(
    interaction: discord.Interaction,
    member_id: str,
    proxy_format: str,
    subsystem_id: str = None,
):
    user_id = interaction.user.id
    system_id = get_user_system_id(user_id)
    if not system_id:
        await interaction.response.send_message("You must register using /register.", ephemeral=True)
        return

    members_dict = get_system_members(system_id, subsystem_id)
    if members_dict is None:
        await interaction.response.send_message("Subsystem not found.", ephemeral=True)
        return
    if member_id not in members_dict:
        await interaction.response.send_message(f"Member not found in {get_scope_label(subsystem_id)}.", ephemeral=True)
        return

    prefix, suffix, err = parse_proxy_format_with_placeholder(proxy_format)
    if err == "missing_placeholder":
        await interaction.response.send_message("Proxy format must include `text` once. Example: `[text]` or `text>>`.", ephemeral=True)
        return
    if err == "multiple_placeholders":
        await interaction.response.send_message("Proxy format can only include `text` once.", ephemeral=True)
        return
    if err == "only_placeholder":
        await interaction.response.send_message("Proxy format cannot be just `text`; add a prefix or suffix.", ephemeral=True)
        return
    if err == "empty":
        await interaction.response.send_message("Proxy format cannot be empty.", ephemeral=True)
        return

    member = members_dict[member_id]
    if not add_member_proxy_format(member, prefix, suffix):
        await interaction.response.send_message("That proxy format is already on this member.", ephemeral=True)
        return

    save_systems()
    await interaction.response.send_message(
        f"Added proxy format for **{member.get('name', member_id)}** in {get_scope_label(subsystem_id)}.\nCurrent formats:\n{render_member_proxy_format(member)}",
        ephemeral=True,
    )


@tree.command(name="removemembertag", description="Remove one proxy format from a member")
@app_commands.autocomplete(subsystem_id=subsystem_id_autocomplete)
async def removemembertag(
    interaction: discord.Interaction,
    member_id: str,
    proxy_format: str,
    subsystem_id: str = None,
):
    user_id = interaction.user.id
    system_id = get_user_system_id(user_id)
    if not system_id:
        await interaction.response.send_message("You must register using /register.", ephemeral=True)
        return

    members_dict = get_system_members(system_id, subsystem_id)
    if members_dict is None:
        await interaction.response.send_message("Subsystem not found.", ephemeral=True)
        return
    if member_id not in members_dict:
        await interaction.response.send_message(f"Member not found in {get_scope_label(subsystem_id)}.", ephemeral=True)
        return

    prefix, suffix, err = parse_proxy_format_with_placeholder(proxy_format)
    if err == "missing_placeholder":
        await interaction.response.send_message("Proxy format must include `text` once. Example: `[text]` or `text>>`.", ephemeral=True)
        return
    if err == "multiple_placeholders":
        await interaction.response.send_message("Proxy format can only include `text` once.", ephemeral=True)
        return
    if err == "only_placeholder":
        await interaction.response.send_message("Proxy format cannot be just `text`; add a prefix or suffix.", ephemeral=True)
        return
    if err == "empty":
        await interaction.response.send_message("Proxy format cannot be empty.", ephemeral=True)
        return

    member = members_dict[member_id]
    if not remove_member_proxy_format(member, prefix, suffix):
        await interaction.response.send_message("That proxy format was not found on this member.", ephemeral=True)
        return

    save_systems()
    await interaction.response.send_message(
        f"Removed proxy format for **{member.get('name', member_id)}** in {get_scope_label(subsystem_id)}.\nCurrent formats:\n{render_member_proxy_format(member)}",
        ephemeral=True,
    )
# -----------------------------
# Edit member images
# -----------------------------
@tree.command(name="editmemberimages", description="Edit a member's profile picture or banner in a subsystem")
@app_commands.autocomplete(subsystem_id=subsystem_id_autocomplete)
async def editmemberimages(
    interaction: discord.Interaction,
    member_id: str,
    subsystem_id: str = None,
    profile_pic: discord.Attachment = None,
    banner: discord.Attachment = None
):
    user_id = interaction.user.id
    system_id = get_user_system_id(user_id)
    if not system_id:
        await interaction.response.send_message("You must register using /register.", ephemeral=True)
        return
    
    members_dict = get_system_members(system_id, subsystem_id)
    if members_dict is None:
        await interaction.response.send_message("Subsystem not found.", ephemeral=True)
        return
    
    if member_id not in members_dict:
        await interaction.response.send_message(f"Member not found in {get_scope_label(subsystem_id)}.", ephemeral=True)
        return

    member = members_dict[member_id]

    warnings = []

    if profile_pic:
        member["profile_pic"] = profile_pic.url
        if is_ephemeral_discord_attachment_url(profile_pic.url):
            warnings.append("profile pic")
    if banner:
        member["banner"] = banner.url
        if is_ephemeral_discord_attachment_url(banner.url):
            warnings.append("banner")

    save_systems()
    msg = f"Updated profile/banner for **{member['name']}**."
    if warnings:
        joined = ", ".join(warnings)
        msg += (
            "\nNote: Discord provided an expiring attachment URL for "
            f"{joined}. If images disappear later, re-upload using prefix command "
            "`Cor;editmemberimages <member_id> [subsystem_id]` with image attachment(s)."
        )
    await interaction.response.send_message(msg)

# -----------------------------
# List all members
# -----------------------------
@tree.command(name="members", description="View members with paging (main, subsystem, or whole system)")
@app_commands.autocomplete(subsystem_id=subsystem_id_autocomplete)
async def members_list(
    interaction: discord.Interaction,
    subsystem_id: str = None,
    whole_system: bool = False,
    target_user_id: str = None,
):
    requester_id = interaction.user.id
    system_id, system, target_owner_id, error = resolve_target_system_for_view(requester_id, target_user_id)
    if error:
        await interaction.response.send_message(error, ephemeral=True)
        return

    if str(target_owner_id) != str(requester_id) and not can_view_system_data(system, requester_id):
        await interaction.response.send_message("You do not have permission to view this member list.", ephemeral=True)
        return

    if whole_system:
        member_rows = []
        scoped_members_lookup = {}
        for scope_id, scoped_members in iter_system_member_dicts(system):
            scoped_members_lookup[scope_id] = scoped_members
            for member_id, member in scoped_members.items():
                if str(target_owner_id) != str(requester_id) and not can_view_member_data(system, member, requester_id):
                    continue
                member_rows.append((scope_id, member_id, member))
        title_scope = "Entire System"
    else:
        members_dict = get_system_members(system_id, subsystem_id)
        if members_dict is None:
            await interaction.response.send_message("Subsystem not found.", ephemeral=True)
            return
        scoped_members_lookup = {subsystem_id: members_dict}
        member_rows = []
        for member_id, member in members_dict.items():
            if str(target_owner_id) != str(requester_id) and not can_view_member_data(system, member, requester_id):
                continue
            member_rows.append((subsystem_id, member_id, member))
        title_scope = get_scope_label(subsystem_id).capitalize()

    if not member_rows:
        await interaction.response.send_message("No visible members found.", ephemeral=True)
        return

    sort_mode = get_member_sort_mode(system)
    member_rows = sort_member_rows(member_rows, sort_mode)
    
    members_per_page = 15
    total_pages = (len(member_rows) - 1) // members_per_page + 1 if member_rows else 1

    page = 1  # start at page 1

    def get_embed(page):
        start_idx = (page - 1) * members_per_page
        end_idx = start_idx + members_per_page
        page_members = member_rows[start_idx:end_idx]

        desc_lines = []
        for scope_id, member_id, m in page_members:
            current_front = m.get("current_front")
            fronting = "Yes" if current_front else "No"
            duration = format_duration(calculate_front_duration(m))
            scoped_members = scoped_members_lookup.get(scope_id, {})
            cofront_ids = current_front.get("cofronts", []) if current_front else []
            cofront_names = [scoped_members.get(co_id, {}).get("name", str(co_id)) for co_id in cofront_ids]
            co_fronts = ", ".join(cofront_names) if cofront_names else "None"
            scope_label = get_scope_label(scope_id)
            desc_lines.append(
                f"**{m['name']}** (ID `{member_id}`) | Scope: {scope_label} | Fronting: {fronting} | Co-fronts: {co_fronts} | Total Front Time: {duration}"
            )

        embed = discord.Embed(
            title=f"Members List - {title_scope} (Page {page}/{total_pages})",
            description="\n".join(desc_lines) or "No members found.",
            color=0x00FF00
        )
        embed.set_footer(text=f"Sort: {'Alphabetical' if sort_mode == 'alphabetical' else 'ID'}")
        return embed

    # Initial embed
    embed = get_embed(page)

    # Buttons for pagination
    class Paginator(discord.ui.View):
        def __init__(self):
            super().__init__(timeout=60)
            self.current_page = page

        @discord.ui.button(label="Previous", style=discord.ButtonStyle.primary)
        async def prev(self, interaction: discord.Interaction, button: discord.ui.Button):
            if self.current_page > 1:
                self.current_page -= 1
                await interaction.response.edit_message(embed=get_embed(self.current_page), view=self)

        @discord.ui.button(label="Next", style=discord.ButtonStyle.primary)
        async def next(self, interaction: discord.Interaction, button: discord.ui.Button):
            if self.current_page < total_pages:
                self.current_page += 1
                await interaction.response.edit_message(embed=get_embed(self.current_page), view=self)

    await interaction.response.send_message(embed=embed, view=Paginator())


@tree.command(name="membersort", description="Set member list sort mode")
@app_commands.choices(mode=[
    app_commands.Choice(name="ID", value="id"),
    app_commands.Choice(name="Alphabetical", value="alphabetical"),
])
async def membersort(interaction: discord.Interaction, mode: str):
    user_id = interaction.user.id
    system_id = get_user_system_id(user_id)
    if not system_id:
        await interaction.response.send_message("You must register a main system first using /register.", ephemeral=True)
        return

    system = systems_data.get("systems", {}).get(system_id)
    if not system:
        await interaction.response.send_message("System not found.", ephemeral=True)
        return

    settings = system.setdefault("member_display", {})
    settings["sort_mode"] = mode
    save_systems()
    label = "Alphabetical" if mode == "alphabetical" else "ID"
    await interaction.response.send_message(f"Member list sorting set to **{label}**.", ephemeral=True)


@tree.command(name="creategroup", description="Create a member group (optionally nested under another group)")
async def creategroup(interaction: discord.Interaction, name: str, parent_group_id: str = None):
    user_id = interaction.user.id
    system_id = get_user_system_id(user_id)
    if not system_id:
        await interaction.response.send_message("You must register a main system first using /register.", ephemeral=True)
        return

    system = systems_data.get("systems", {}).get(system_id)
    if not system:
        await interaction.response.send_message("System not found.", ephemeral=True)
        return

    settings = get_group_settings(system)
    groups = settings.get("groups", {})

    if parent_group_id is not None:
        parent_group_id = str(parent_group_id)
        if parent_group_id not in groups:
            await interaction.response.send_message("Parent group not found.", ephemeral=True)
            return

    group_id = get_next_group_id(system)
    groups[group_id] = {
        "id": group_id,
        "name": name.strip() or f"Group {group_id}",
        "parent_id": parent_group_id,
    }
    settings.setdefault("order", []).append(group_id)
    save_systems()

    parent_msg = f" under `{parent_group_id}`" if parent_group_id else ""
    await interaction.response.send_message(f"Created group **{groups[group_id]['name']}** (`{group_id}`){parent_msg}.", ephemeral=True)


@tree.command(name="editgroup", description="Edit group name or parent for nesting")
async def editgroup(
    interaction: discord.Interaction,
    group_id: str,
    name: str = None,
    parent_group_id: str = None,
    clear_parent: bool = False,
):
    user_id = interaction.user.id
    system_id = get_user_system_id(user_id)
    if not system_id:
        await interaction.response.send_message("You must register a main system first using /register.", ephemeral=True)
        return

    system = systems_data.get("systems", {}).get(system_id)
    if not system:
        await interaction.response.send_message("System not found.", ephemeral=True)
        return

    settings = get_group_settings(system)
    groups = settings.get("groups", {})
    group_id = str(group_id)
    group = groups.get(group_id)
    if not group:
        await interaction.response.send_message("Group not found.", ephemeral=True)
        return

    if name is not None:
        cleaned = name.strip()
        if cleaned:
            group["name"] = cleaned

    if clear_parent:
        group["parent_id"] = None
    elif parent_group_id is not None:
        parent_group_id = str(parent_group_id)
        if parent_group_id not in groups:
            await interaction.response.send_message("Parent group not found.", ephemeral=True)
            return
        if parent_group_id == group_id:
            await interaction.response.send_message("A group cannot be its own parent.", ephemeral=True)
            return

        # Prevent cycles in nested hierarchy.
        check_id = parent_group_id
        seen = set()
        while check_id and check_id not in seen:
            seen.add(check_id)
            if check_id == group_id:
                await interaction.response.send_message("Invalid parent: this would create a cycle.", ephemeral=True)
                return
            next_parent = groups.get(check_id, {}).get("parent_id")
            check_id = str(next_parent) if next_parent is not None else None

        group["parent_id"] = parent_group_id

    save_systems()
    await interaction.response.send_message(f"Updated group **{group['name']}** (`{group_id}`).", ephemeral=True)


@tree.command(name="deletegroup", description="Delete a group and optionally all nested child groups")
async def deletegroup(interaction: discord.Interaction, group_id: str, delete_children: bool = True):
    user_id = interaction.user.id
    system_id = get_user_system_id(user_id)
    if not system_id:
        await interaction.response.send_message("You must register a main system first using /register.", ephemeral=True)
        return

    system = systems_data.get("systems", {}).get(system_id)
    if not system:
        await interaction.response.send_message("System not found.", ephemeral=True)
        return

    settings = get_group_settings(system)
    groups = settings.get("groups", {})
    group_id = str(group_id)
    if group_id not in groups:
        await interaction.response.send_message("Group not found.", ephemeral=True)
        return

    to_delete = {group_id}
    if delete_children:
        changed = True
        while changed:
            changed = False
            for gid, group in groups.items():
                parent_id = group.get("parent_id")
                if parent_id is not None and str(parent_id) in to_delete and gid not in to_delete:
                    to_delete.add(gid)
                    changed = True

    # Remove from all member assignments across main + subsystems.
    for _, members_dict in iter_system_member_dicts(system):
        for member in members_dict.values():
            existing = member.get("groups", []) or []
            member["groups"] = [str(gid) for gid in existing if str(gid) not in to_delete]

    for gid in list(to_delete):
        groups.pop(gid, None)
    settings["order"] = [gid for gid in settings.get("order", []) if gid not in to_delete]
    save_systems()

    await interaction.response.send_message(
        f"Deleted {len(to_delete)} group(s): {', '.join(sorted(to_delete))}",
        ephemeral=True,
    )


@tree.command(name="listgroups", description="List your groups in current card display order")
async def listgroups_cmd(interaction: discord.Interaction, show_to_others: bool = False):
    user_id = interaction.user.id
    system_id = get_user_system_id(user_id)
    if not system_id:
        await interaction.response.send_message("You must register a main system first using /register.", ephemeral=True)
        return

    system = systems_data.get("systems", {}).get(system_id)
    if not system:
        await interaction.response.send_message("System not found.", ephemeral=True)
        return

    settings = get_group_settings(system)
    groups = settings.get("groups", {})
    order = settings.get("order", [])
    if not groups:
        await interaction.response.send_message("No groups found. Use /creategroup to add one.", ephemeral=True)
        return

    lines = []
    for gid in order:
        if gid not in groups:
            continue
        path = get_group_path_text(groups, gid)
        lines.append(f"- `{gid}`: {path}")

    embed = discord.Embed(
        title="Group List",
        description="\n".join(lines) if lines else "No groups found.",
        color=discord.Color.dark_teal(),
    )
    await interaction.response.send_message(embed=embed, ephemeral=not show_to_others)


@tree.command(name="grouporder", description="Set the order groups appear on member cards")
async def grouporder(interaction: discord.Interaction, group_ids: str):
    user_id = interaction.user.id
    system_id = get_user_system_id(user_id)
    if not system_id:
        await interaction.response.send_message("You must register a main system first using /register.", ephemeral=True)
        return

    system = systems_data.get("systems", {}).get(system_id)
    if not system:
        await interaction.response.send_message("System not found.", ephemeral=True)
        return

    settings = get_group_settings(system)
    groups = settings.get("groups", {})
    tokens = [t.strip() for t in group_ids.replace(";", ",").split(",") if t.strip()]
    if not tokens:
        await interaction.response.send_message("Provide at least one group ID.", ephemeral=True)
        return

    unknown = [gid for gid in tokens if gid not in groups]
    if unknown:
        await interaction.response.send_message(f"Unknown group IDs: {', '.join(unknown)}", ephemeral=True)
        return

    new_order = []
    seen = set()
    for gid in tokens:
        if gid not in seen:
            new_order.append(gid)
            seen.add(gid)
    for gid in settings.get("order", []):
        if gid in groups and gid not in seen:
            new_order.append(gid)
            seen.add(gid)

    settings["order"] = new_order
    save_systems()
    await interaction.response.send_message(f"Group order updated: {', '.join(new_order)}", ephemeral=True)


@tree.command(name="grouporderui", description="Interactively reorder groups for member card display")
async def grouporderui(interaction: discord.Interaction):
    user_id = interaction.user.id
    system_id = get_user_system_id(user_id)
    if not system_id:
        await interaction.response.send_message("You must register a main system first using /register.", ephemeral=True)
        return

    system = systems_data.get("systems", {}).get(system_id)
    if not system:
        await interaction.response.send_message("System not found.", ephemeral=True)
        return

    settings = get_group_settings(system)
    if not settings.get("groups"):
        await interaction.response.send_message("No groups found. Use /creategroup to add one.", ephemeral=True)
        return

    view = GroupOrderView(interaction.user.id, system)
    await interaction.response.send_message(embed=view.current_embed(), view=view, ephemeral=True)


@tree.command(name="addmembergroup", description="Assign a group to a member")
@app_commands.autocomplete(subsystem_id=subsystem_id_autocomplete)
async def addmembergroup(interaction: discord.Interaction, member_id: str, group_id: str, subsystem_id: str = None):
    user_id = interaction.user.id
    system_id = get_user_system_id(user_id)
    if not system_id:
        await interaction.response.send_message("You must register using /register.", ephemeral=True)
        return

    system = systems_data.get("systems", {}).get(system_id)
    members_dict = get_system_members(system_id, subsystem_id)
    if members_dict is None:
        await interaction.response.send_message("Subsystem not found.", ephemeral=True)
        return
    if member_id not in members_dict:
        await interaction.response.send_message(f"Member not found in {get_scope_label(subsystem_id)}.", ephemeral=True)
        return

    groups = get_group_settings(system).get("groups", {})
    group_id = str(group_id)
    if group_id not in groups:
        await interaction.response.send_message("Group not found.", ephemeral=True)
        return

    member = members_dict[member_id]
    assigned = [str(gid) for gid in (member.get("groups", []) or [])]
    if group_id not in assigned:
        assigned.append(group_id)
        member["groups"] = assigned
        save_systems()

    await interaction.response.send_message(f"Added group `{group_id}` to **{member['name']}**.", ephemeral=True)


@tree.command(name="removemembergroup", description="Remove a group from a member")
@app_commands.autocomplete(subsystem_id=subsystem_id_autocomplete)
async def removemembergroup(interaction: discord.Interaction, member_id: str, group_id: str, subsystem_id: str = None):
    user_id = interaction.user.id
    system_id = get_user_system_id(user_id)
    if not system_id:
        await interaction.response.send_message("You must register using /register.", ephemeral=True)
        return

    members_dict = get_system_members(system_id, subsystem_id)
    if members_dict is None:
        await interaction.response.send_message("Subsystem not found.", ephemeral=True)
        return
    if member_id not in members_dict:
        await interaction.response.send_message(f"Member not found in {get_scope_label(subsystem_id)}.", ephemeral=True)
        return

    member = members_dict[member_id]
    group_id = str(group_id)
    assigned = [str(gid) for gid in (member.get("groups", []) or [])]
    if group_id not in assigned:
        await interaction.response.send_message(f"Member is not in group `{group_id}`.", ephemeral=True)
        return

    member["groups"] = [gid for gid in assigned if gid != group_id]
    save_systems()
    await interaction.response.send_message(f"Removed group `{group_id}` from **{member['name']}**.", ephemeral=True)


@tree.command(name="membergroups", description="List groups assigned to a member")
@app_commands.autocomplete(subsystem_id=subsystem_id_autocomplete)
async def membergroups(interaction: discord.Interaction, member_id: str, subsystem_id: str = None):
    user_id = interaction.user.id
    system_id = get_user_system_id(user_id)
    if not system_id:
        await interaction.response.send_message("You must register using /register.", ephemeral=True)
        return

    system = systems_data.get("systems", {}).get(system_id)
    members_dict = get_system_members(system_id, subsystem_id)
    if members_dict is None:
        await interaction.response.send_message("Subsystem not found.", ephemeral=True)
        return
    if member_id not in members_dict:
        await interaction.response.send_message(f"Member not found in {get_scope_label(subsystem_id)}.", ephemeral=True)
        return

    member = members_dict[member_id]
    embed = discord.Embed(
        title=f"Groups - {member.get('name', member_id)}",
        description=format_member_group_lines(system, member),
        color=discord.Color.teal(),
    )
    await interaction.response.send_message(embed=embed, ephemeral=True)

# -----------------------------
# Search Members
# -----------------------------
@tree.command(name="searchmember", description="Search for a member by name or tag")
@app_commands.autocomplete(subsystem_id=subsystem_id_autocomplete, name=member_name_autocomplete, tag=tag_autocomplete)
async def searchmember(interaction: discord.Interaction, subsystem_id: str = None, name: str = None, tag: str = None):
    user_id = interaction.user.id
    system_id = get_user_system_id(user_id)
    if not system_id:
        await interaction.response.send_message("You must register using /register.", ephemeral=True)
        return
    system = systems_data.get("systems", {}).get(system_id)
    if not system:
        await interaction.response.send_message("System not found.", ephemeral=True)
        return
    
    members_dict = get_system_members(system_id, subsystem_id)
    if members_dict is None:
        await interaction.response.send_message("Subsystem not found.", ephemeral=True)
        return

    if not name and not tag:
        await interaction.response.send_message("Provide a name or tag to search.", ephemeral=True)
        return

    results = list(members_dict.values())

    if name:
        results = [m for m in results if name.lower() in m["name"].lower()]

    if tag:
        results = [m for m in results if tag.lower() in [t.lower() for t in m.get("tags", [])]]

    if not results:
        await interaction.response.send_message("No members found.", ephemeral=True)
        return

    if len(results) == 1:
        member = results[0]
        fronting = "Yes" if member.get("current_front") else "No"
        duration = format_duration(calculate_front_duration(member))
        co_fronts = ", ".join(member.get("co_fronts", [])) if member.get("co_fronts") else "None"

        embed = discord.Embed(
            title=member["name"],
            description=member.get("description", "No description."),
            color=int(member.get("color", "FFFFFF"), 16)
        )

        embed.add_field(name="Currently Fronting", value=fronting, inline=True)
        embed.add_field(name="Co-fronts", value=co_fronts, inline=True)
        embed.add_field(name="Total Front Time", value=duration, inline=False)

        tags = ", ".join(member.get("tags", [])) if member.get("tags") else "None"
        embed.add_field(name="Tags", value=tags, inline=False)
        embed.add_field(name="Groups", value=format_member_group_lines(system, member), inline=False)

        if member.get("profile_pic"):
            embed.set_thumbnail(url=member["profile_pic"])
        if member.get("banner"):
            embed.set_image(url=member.get("banner"))

        await interaction.response.send_message(embed=embed)
    else:
        lines = []
        for m in results:
            member_tags = ", ".join(m.get("tags", [])) or "None"
            lines.append(f"**{m['name']}** — ID `{m['id']}` — Tags: {member_tags}")

        embed = discord.Embed(
            title=f"Search Results ({len(results)} found)",
            description="\n".join(lines),
            color=discord.Color.blue()
        )
        await interaction.response.send_message(embed=embed)

# -----------------------------
# Tag management
# -----------------------------
@tree.command(name="addtag", description="Add a tag to your system")
async def addtag(interaction: discord.Interaction, tag: str):
    user_id = interaction.user.id
    system_id = get_user_system_id(user_id)
    if not system_id:
        await interaction.response.send_message("You must register using /register.", ephemeral=True)
        return

    tag = tag.strip().lower()

    if tag in PRESET_TAGS:
        await interaction.response.send_message("That tag already exists.", ephemeral=True)
        return

    PRESET_TAGS.append(tag)
    save_tags()

    await interaction.response.send_message(f"Tag `{tag}` added.")

# -----------------------------
# List tags
# -----------------------------
@tree.command(name="listtags", description="List all tags in your system")
async def listtags(interaction: discord.Interaction):
    user_id = interaction.user.id
    system_id = get_user_system_id(user_id)
    if not system_id:
        await interaction.response.send_message("You must register using /register.", ephemeral=True)
        return

    if not PRESET_TAGS:
        await interaction.response.send_message("No tags exist.")
        return

    tag_lines = "\n".join([f"• {tag}" for tag in sorted(PRESET_TAGS, key=lambda x: x.lower())])

    embed = discord.Embed(
        title="System Tags",
        description=tag_lines,
        color=discord.Color.green()
    )

    await interaction.response.send_message(embed=embed)

# -----------------------------
# Confirmation buttons for dangerous actions
# -----------------------------
class ConfirmAction(discord.ui.View):
    def __init__(self, confirm_callback):
        super().__init__(timeout=30)
        self.confirm_callback = confirm_callback

    @discord.ui.button(label="Confirm", style=discord.ButtonStyle.danger)
    async def confirm(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.confirm_callback(interaction)
        self.stop()

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_message("Action cancelled.", ephemeral=True)
        self.stop()


# Example usage for clearfront:
@tree.command(name="clearfront", description="Clear fronting status in a subsystem")
@app_commands.autocomplete(subsystem_id=subsystem_id_autocomplete)
async def clearfront(interaction: discord.Interaction, subsystem_id: str = None):
    user_id = interaction.user.id
    system_id = get_user_system_id(user_id)
    if not system_id:
        await interaction.response.send_message("You must register using /register.", ephemeral=True)
        return

    members_dict = get_system_members(system_id, subsystem_id)
    if members_dict is None:
        await interaction.response.send_message("Subsystem not found.", ephemeral=True)
        return

    async def do_clear(interaction: discord.Interaction):
        any_fronting = False
        for m_id, m in members_dict.items():
            if m.get("current_front"):
                end_front(m_id, members_dict=members_dict)
                any_fronting = True
        save_systems()
        if any_fronting:
            await interaction.response.send_message(f"All members are now removed from front in {get_scope_label(subsystem_id)}.", ephemeral=True)
        else:
            await interaction.response.send_message(f"No members were fronting in {get_scope_label(subsystem_id)}.", ephemeral=True)

    view = ConfirmAction(confirm_callback=do_clear)
    await interaction.response.send_message(f"Are you sure you want to clear all current fronts in {get_scope_label(subsystem_id)}?", view=view, ephemeral=True)


# Example usage for removetag:
@tree.command(name="removetag", description="Remove a tag from your system")
async def removetag(interaction: discord.Interaction, tag: str):
    user_id = interaction.user.id
    system_id = get_user_system_id(user_id)
    if not system_id:
        await interaction.response.send_message("You must register using /register.", ephemeral=True)
        return

    tag = tag.strip().lower()

    if tag not in PRESET_TAGS:
        await interaction.response.send_message("Tag not found.", ephemeral=True)
        return

    async def do_remove(interaction: discord.Interaction):
        PRESET_TAGS.remove(tag)
        # Remove tag from all members across all subsystems in user's system
        if system_id in systems_data["systems"]:
            system = systems_data["systems"][system_id]
            for member in system["members"].values():
                if tag in member.get("tags", []):
                    member["tags"].remove(tag)
            for subsystem in system.get("subsystems", {}).values():
                for member in subsystem["members"].values():
                    if tag in member.get("tags", []):
                        member["tags"].remove(tag)
        save_tags()
        save_systems()
        await interaction.response.send_message(f"Tag `{tag}` removed.", ephemeral=True)

    view = ConfirmAction(confirm_callback=do_remove)
    await interaction.response.send_message(f"Are you sure you want to remove the tag `{tag}` from your system?", view=view, ephemeral=True)
# -----------------------------
# Remove member
# -----------------------------
class ConfirmRemove(discord.ui.View):
    def __init__(self, member_id, system_id, subsystem_id):
        super().__init__(timeout=30)
        self.member_id = member_id
        self.system_id = system_id
        self.subsystem_id = subsystem_id

    @discord.ui.button(label="Confirm", style=discord.ButtonStyle.danger)
    async def confirm(self, interaction: discord.Interaction, button: discord.ui.Button):
        members_dict = get_system_members(self.system_id, self.subsystem_id)
        if members_dict and self.member_id in members_dict:
            name = members_dict[self.member_id]["name"]
            del members_dict[self.member_id]
            save_systems()
            await interaction.response.edit_message(content=f"Member **{name}** removed.", view=None)
        else:
            await interaction.response.edit_message(content="Member not found.", view=None)

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.edit_message(content="Action cancelled.", view=None)

@tree.command(name="removemember", description="Remove a member from a subsystem")
@app_commands.autocomplete(subsystem_id=subsystem_id_autocomplete)
async def removemember(interaction: discord.Interaction, member_id: str, subsystem_id: str = None):
    user_id = interaction.user.id
    system_id = get_user_system_id(user_id)
    if not system_id:
        await interaction.response.send_message("You must register using /register.", ephemeral=True)
        return
    
    members_dict = get_system_members(system_id, subsystem_id)
    if members_dict is None:
        await interaction.response.send_message("Subsystem not found.", ephemeral=True)
        return
    
    if member_id not in members_dict:
        await interaction.response.send_message(f"Member not found in {get_scope_label(subsystem_id)}.", ephemeral=True)
        return
    
    await interaction.response.send_message(
        f"Are you sure you want to remove **{members_dict[member_id]['name']}**?",
        view=ConfirmRemove(member_id, system_id, subsystem_id),
        ephemeral=True
    )
# -----------------------------
# Multi-member removal
# -----------------------------
class MultiMemberSelect(discord.ui.Select):
    def __init__(self, parent_view):
        self.parent_view = parent_view
        options = self.parent_view.build_page_options()
        super().__init__(
            placeholder="Select members to remove...",
            min_values=0,
            max_values=len(options) if options else 1,
            options=options
        )

    async def callback(self, interaction: discord.Interaction):
        page_member_ids = {opt.value for opt in self.options}
        self.parent_view.selected_ids.difference_update(page_member_ids)
        self.parent_view.selected_ids.update(self.values)
        await interaction.response.defer()

class MultiMemberView(discord.ui.View):
    def __init__(self, members_dict):
        super().__init__(timeout=120)
        self.members_dict = members_dict
        self.member_items = sorted(
            [(member_id, member) for member_id, member in members_dict.items()],
            key=lambda item: item[1].get("name", "").lower()
        )
        self.page_size = 25
        self.current_page = 0
        self.total_pages = max(1, (len(self.member_items) - 1) // self.page_size + 1)
        self.selected_ids = set()
        self.cancelled = False

        self.member_select = MultiMemberSelect(self)
        self.add_item(self.member_select)

    def current_page_items(self):
        start = self.current_page * self.page_size
        end = start + self.page_size
        return self.member_items[start:end]

    def build_page_options(self):
        options = []
        for member_id, member in self.current_page_items():
            label = member.get("name", "Unknown")[:100]
            options.append(
                discord.SelectOption(
                    label=label,
                    value=str(member_id),
                    default=(str(member_id) in self.selected_ids)
                )
            )
        return options

    async def refresh_message(self, interaction: discord.Interaction):
        self.remove_item(self.member_select)
        self.member_select = MultiMemberSelect(self)
        self.add_item(self.member_select)
        await interaction.response.edit_message(
            content=(
                f"Select members to remove, then click Confirm. "
                f"Page {self.current_page + 1}/{self.total_pages}. "
                f"Selected: {len(self.selected_ids)}"
            ),
            view=self
        )

    @discord.ui.button(label="Previous", style=discord.ButtonStyle.secondary)
    async def prev_page(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self.current_page > 0:
            self.current_page -= 1
        await self.refresh_message(interaction)

    @discord.ui.button(label="Next", style=discord.ButtonStyle.secondary)
    async def next_page(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self.current_page < self.total_pages - 1:
            self.current_page += 1
        await self.refresh_message(interaction)

    @discord.ui.button(label="Confirm", style=discord.ButtonStyle.green)
    async def confirm(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.stop()
        await interaction.response.defer()

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.danger)
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.cancelled = True
        self.selected_ids.clear()
        self.stop()
        await interaction.response.edit_message(content="Action cancelled.", view=None)

@tree.command(name="removemembers", description="Remove multiple members at once (with confirmation) from a subsystem")
@app_commands.autocomplete(subsystem_id=subsystem_id_autocomplete)
async def removemembers(interaction: discord.Interaction, subsystem_id: str = None):
    user_id = interaction.user.id
    system_id = get_user_system_id(user_id)
    if not system_id:
        await interaction.response.send_message("You must register using /register.", ephemeral=True)
        return
    
    members_dict = get_system_members(system_id, subsystem_id)
    if members_dict is None:
        await interaction.response.send_message("Subsystem not found.", ephemeral=True)
        return
    
    if not members_dict:
        await interaction.response.send_message(f"No members exist to remove in {get_scope_label(subsystem_id)}.", ephemeral=True)
        return

    view = MultiMemberView(members_dict)
    await interaction.response.send_message(
        f"Select members to remove, then click Confirm. Page 1/{view.total_pages}. Selected: 0",
        view=view,
        ephemeral=True
    )
    await view.wait()

    if view.cancelled:
        return

    selected_ids = [mid for mid, _ in view.member_items if mid in view.selected_ids]
    if not selected_ids:
        await interaction.followup.send("No members were selected.", ephemeral=True)
        return

    selected_names = [members_dict[mid]["name"] for mid in selected_ids if mid in members_dict]
    
    async def do_remove(interaction: discord.Interaction):
        for mid in selected_ids:
            if mid in members_dict:
                del members_dict[mid]
        save_systems()
        await interaction.response.send_message(
            f"Removed members: {', '.join(selected_names)}",
            ephemeral=True
        )

    confirm_view = ConfirmAction(confirm_callback=do_remove)
    await interaction.followup.send(
        f"Are you sure you want to remove these members? {', '.join(selected_names)}",
        view=confirm_view,
        ephemeral=True
    )

# -----------------------------
# Clear all members (dangerous!)
# -----------------------------
class ConfirmClearSystem(discord.ui.View):
    def __init__(self, system):
        super().__init__(timeout=30)
        self.system = system

    @discord.ui.button(label="Confirm", style=discord.ButtonStyle.danger)
    async def confirm(self, interaction: discord.Interaction, button: discord.ui.Button):
        # Clear main system members
        self.system["members"] = {}

        # Clear all subsystem members
        for subsystem in self.system.get("subsystems", {}).values():
            subsystem["members"] = {}

        save_systems()
        await interaction.response.edit_message(
            content="All members have been removed from your entire system (main + all subsystems).",
            view=None
        )

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.edit_message(content="Action cancelled.", view=None)


@tree.command(name="clearsystem", description="Remove all members from your entire system (dangerous!)")
async def clearsystem(interaction: discord.Interaction):
    user_id = interaction.user.id
    system_id = get_user_system_id(user_id)
    if not system_id:
        await interaction.response.send_message("You must register using /register.", ephemeral=True)
        return

    system = systems_data["systems"].get(system_id)
    if not system:
        await interaction.response.send_message("System not found.", ephemeral=True)
        return
    
    await interaction.response.send_message(
        "Are you sure you want to remove **all members** from your entire system (main + all subsystems)?",
        view=ConfirmClearSystem(system),
        ephemeral=True
    )


@tree.command(name="clearsubsystem", description="Remove all members from one subsystem (dangerous!)")
@app_commands.choices(action=[
    app_commands.Choice(name="Delete all members", value="delete"),
    app_commands.Choice(name="Move members to main system", value="move")
])
@app_commands.autocomplete(subsystem_id=subsystem_id_autocomplete)
async def clearsubsystem(interaction: discord.Interaction, subsystem_id: str = None, action: str = "delete"):
    user_id = interaction.user.id
    system_id = get_user_system_id(user_id)
    if not system_id:
        await interaction.response.send_message("You must register using /register.", ephemeral=True)
        return

    if subsystem_id is None:
        await interaction.response.send_message("Provide a subsystem ID for /clearsubsystem, or use /clearsystem for the entire system.", ephemeral=True)
        return

    members_dict = get_system_members(system_id, subsystem_id)
    if members_dict is None:
        await interaction.response.send_message("Subsystem not found.", ephemeral=True)
        return

    if action not in {"delete", "move"}:
        await interaction.response.send_message("Invalid action. Use either `delete` or `move`.", ephemeral=True)
        return

    system = systems_data["systems"].get(system_id, {})
    main_members = system.setdefault("members", {})

    async def do_clear_subsystem(interaction: discord.Interaction):
        if action == "move":
            moved_count = 0

            for _, member in list(members_dict.items()):
                new_id = get_next_system_member_id(system_id)
                member["id"] = new_id
                main_members[new_id] = member
                moved_count += 1

            members_dict.clear()
            save_systems()
            await interaction.response.send_message(
                f"Moved **{moved_count}** member(s) from subsystem `{subsystem_id}` to your main system.",
                ephemeral=True
            )
            return

        deleted_count = len(members_dict)
        members_dict.clear()
        save_systems()
        await interaction.response.send_message(
            f"Deleted **{deleted_count}** member(s) from subsystem `{subsystem_id}`.",
            ephemeral=True
        )

    mode_text = "move all members to your main system" if action == "move" else "delete all members"
    view = ConfirmAction(confirm_callback=do_clear_subsystem)
    await interaction.response.send_message(
        f"Are you sure you want to {mode_text} for subsystem `{subsystem_id}`?",
        view=view,
        ephemeral=True
    )


@tree.command(name="deletesystem", description="Delete your entire registered system record (dangerous!)")
async def deletesystem(interaction: discord.Interaction):
    user_id = interaction.user.id
    system_id = get_user_system_id(user_id)
    if not system_id:
        await interaction.response.send_message("You do not have a registered system.", ephemeral=True)
        return

    system_name = systems_data["systems"].get(system_id, {}).get("system_name", f"System {system_id}")

    async def do_delete(interaction: discord.Interaction):
        removed = systems_data["systems"].pop(system_id, None)
        if removed is None:
            await interaction.response.send_message("System not found. It may have already been deleted.", ephemeral=True)
            return
        save_systems()
        await interaction.response.send_message(
            f"System **{system_name}** (ID `{system_id}`) was deleted.",
            ephemeral=True
        )

    view = ConfirmAction(confirm_callback=do_delete)
    await interaction.response.send_message(
        f"Are you sure you want to permanently delete system **{system_name}** (ID `{system_id}`)?",
        view=view,
        ephemeral=True
    )
# -----------------------------
# Clear all front history
# -----------------------------
@tree.command(name="clearall", description="Clear all front history for members in a subsystem")
@app_commands.autocomplete(subsystem_id=subsystem_id_autocomplete)
async def clearall(interaction: discord.Interaction, subsystem_id: str = None):
    user_id = interaction.user.id
    system_id = get_user_system_id(user_id)
    if not system_id:
        await interaction.response.send_message("You must register using /register.", ephemeral=True)
        return
    
    members_dict = get_system_members(system_id, subsystem_id)
    if members_dict is None:
        await interaction.response.send_message("Subsystem not found.", ephemeral=True)
        return
    
    async def do_clear(interaction: discord.Interaction):
        for m in members_dict.values():
            m["front_history"] = []
            m["fronting"] = False
            m["cofronting"] = []
            m["fronting_since"] = None
        save_systems()
        await interaction.response.send_message(f"All front history has been cleared from {get_scope_label(subsystem_id)}.", ephemeral=True)

    view = ConfirmAction(confirm_callback=do_clear)
    await interaction.response.send_message(
        f"Are you sure you want to clear all front history in {get_scope_label(subsystem_id)}? This cannot be undone.",
        view=view,
        ephemeral=True
    )
# -----------------------------
# Sync
# -----------------------------
# -----------------------------
# Refresh databases
# -----------------------------
@tree.command(name="refresh", description="Reload all databases from disk")
@app_commands.default_permissions(administrator=True)
async def refresh(interaction: discord.Interaction):
    global systems_data, PRESET_TAGS

    if os.path.exists(JSON_FILE):
        with open(JSON_FILE, "r") as f:
            systems_data = json.load(f)
    else:
        systems_data = {"systems": {}}

    if os.path.exists(TAGS_FILE):
        with open(TAGS_FILE, "r") as f:
            PRESET_TAGS.clear()
            PRESET_TAGS.extend(json.load(f))
    else:
        PRESET_TAGS.clear()

    total_members = sum(len(system.get("members", {})) + sum(len(sub["members"]) for sub in system.get("subsystems", {}).values()) for system in systems_data.get("systems", {}).values())
    await interaction.response.send_message(
        f"Databases refreshed. Loaded **{len(systems_data.get('systems', {}))}** systems with **{total_members}** total members and **{len(PRESET_TAGS)}** tags."
    )

# -----------------------------
# Sync
# -----------------------------
@tree.command(name="synccommands", description="Force sync all commands globally")
@app_commands.default_permissions(administrator=True)
async def synccommands(interaction: discord.Interaction):
    await interaction.response.defer()

    removed_guild_scoped = 0
    # Clear any legacy guild-scoped copies to avoid duplicate entries in the slash menu.
    if interaction.guild is not None:
        existing_guild_commands = tree.get_commands(guild=interaction.guild)
        removed_guild_scoped = len(existing_guild_commands)
        tree.clear_commands(guild=interaction.guild)
        await tree.sync(guild=interaction.guild)

    global_synced = await tree.sync()

    await interaction.followup.send(
        (
            f"Commands synced globally: {len(global_synced)}. "
            f"Removed {removed_guild_scoped} guild-scoped command copies in this server. "
            "Global updates may take time to propagate."
        )
    )

@tasks.loop(minutes=5)
async def front_reminder_loop():
    any_updates = False
    now = datetime.now(timezone.utc)

    for system in systems_data.get("systems", {}).values():
        settings = get_front_reminder_settings(system)
        if not settings.get("enabled"):
            continue

        threshold_seconds = int(settings.get("hours", 4)) * 3600
        owner_id = system.get("owner_id")
        if not owner_id:
            continue

        for scope_id, members_dict in iter_system_member_dicts(system):
            for member in members_dict.values():
                current = member.get("current_front")
                if not current:
                    continue

                current.setdefault("reminder_sent", False)
                current.setdefault("reminded_at", None)
                if current.get("reminder_sent"):
                    continue

                start_iso = current.get("start")
                if not start_iso:
                    continue

                try:
                    start_dt = datetime.fromisoformat(start_iso)
                except ValueError:
                    continue

                elapsed_seconds = (now - start_dt).total_seconds()
                if elapsed_seconds < threshold_seconds:
                    continue

                cofront_names = [
                    members_dict[co_id]["name"]
                    for co_id in current.get("cofronts", [])
                    if co_id in members_dict
                ]
                cofront_text = f" Co-fronts: {', '.join(cofront_names)}." if cofront_names else ""
                scope_text = get_scope_label(scope_id)
                duration_text = format_duration(elapsed_seconds)
                dm_text = (
                    f"Reminder: **{member.get('name', 'Unknown')}** has been fronting in your {scope_text} "
                    f"for **{duration_text}**.{cofront_text}"
                )

                try:
                    user = bot.get_user(int(owner_id)) or await bot.fetch_user(int(owner_id))
                    await user.send(dm_text)
                except (ValueError, discord.Forbidden, discord.HTTPException):
                    pass

                current["reminder_sent"] = True
                current["reminded_at"] = now.isoformat()
                any_updates = True

    if any_updates:
        save_systems()

@front_reminder_loop.before_loop
async def before_front_reminder_loop():
    await bot.wait_until_ready()

@tasks.loop(minutes=1)
async def scheduled_messages_loop():
    """Check and deliver scheduled messages when their time arrives."""
    now = datetime.now(timezone.utc)
    users_to_cleanup = []
    
    for user_id, messages in SCHEDULED_MESSAGES.items():
        delivered = []
        pending = []
        
        for msg_data in messages:
            try:
                send_at = msg_data["send_at"]
                if isinstance(send_at, str):
                    send_at = datetime.fromisoformat(send_at)
                
                if now >= send_at:
                    try:
                        user = bot.get_user(int(user_id)) or await bot.fetch_user(int(user_id))
                        await user.send(msg_data["message"])
                        delivered.append(msg_data)
                    except (ValueError, discord.Forbidden, discord.HTTPException):
                        pass
                else:
                    pending.append(msg_data)
            except Exception:
                pass
        
        if pending:
            SCHEDULED_MESSAGES[user_id] = pending
        else:
            users_to_cleanup.append(user_id)
    
    for user_id in users_to_cleanup:
        del SCHEDULED_MESSAGES[user_id]

@scheduled_messages_loop.before_loop
async def before_scheduled_messages_loop():
    await bot.wait_until_ready()

@tasks.loop(hours=24)
async def weekly_mood_summary_loop():
    any_updates = False
    now = datetime.now(timezone.utc)

    for system in systems_data.get("systems", {}).values():
        if cleanup_external_inbox_entries(system):
            any_updates = True

        checkins = get_checkin_settings(system)
        if not checkins.get("weekly_dm_enabled", True):
            continue

        local_now = now.astimezone(get_system_timezone(system))
        week_key = current_week_key(local_now)

        if checkins.get("last_weekly_summary_week") == week_key:
            continue

        summary_text = build_weekly_checkin_summary(system)
        owner_id = system.get("owner_id")
        if not owner_id:
            continue

        if summary_text:
            try:
                user = bot.get_user(int(owner_id)) or await bot.fetch_user(int(owner_id))
                await user.send(summary_text)
            except (ValueError, discord.Forbidden, discord.HTTPException):
                pass

        checkins["last_weekly_summary_week"] = week_key
        any_updates = True

    if any_updates:
        save_systems()

@weekly_mood_summary_loop.before_loop
async def before_weekly_mood_summary_loop():
    await bot.wait_until_ready()

# -----------------------------
# Bot ready
# -----------------------------
@bot.event
async def on_ready():
    print(f"Logged in as {bot.user}")

    # Commands are already registered globally — only sync manually via /synccommands
    # Syncing on every restart can trigger Cloudflare rate limits (error 1015)
    print("Skipping automatic command sync (use /synccommands to sync manually)")

    if not front_reminder_loop.is_running():
        front_reminder_loop.start()
    if not weekly_mood_summary_loop.is_running():
        weekly_mood_summary_loop.start()
    if not scheduled_messages_loop.is_running():
        scheduled_messages_loop.start()

    print("Bot is ready!")


# -----------------------------
# Prefix commands (starter set)
# -----------------------------
@bot.command(name="help", aliases=["h"])
async def help_prefix(ctx: commands.Context):
    pages = [
        {
            "title": "Cortex Help - Setup & Cards",
            "description": (
                "Use prefix commands with `Cor;`\n\n"
                "Tip: most commands have short aliases. Example: `Cor;h` for help.\n\n"
                "**Setup**\n"
                "• Cor;register (`reg`) <system_name>\n"
                "• Cor;createsubsystem (`css`) <name>\n"
                "• Cor;editsubsystem (`es`) <subsystem_id> <new_name>\n\n"
                "**Cards**\n"
                "• Cor;viewsystemcard (`vsc`) [target_user_id]\n"
                "• Cor;viewsubsystemcard (`vssc`) <subsystem_id> [target_user_id]\n"
                "• Cor;editsystemcard <fields...>\n"
                "• Cor;editsubsystemcard (`esc`) <subsystem_id> [description|color|set_pic|set_banner|clear_pic|clear_banner]\n"
                "• Cor;listsubsystems (`lss`) [target_user_id]"
            ),
            "color": discord.Color.blurple(),
        },
        {
            "title": "Cortex Help - Identity & Privacy",
            "description": (
                "**Identity**\n"
                "• Cor;systemtag (`stag`) <value|clear>\n"
                "• Cor;serveridentity (`si`) <display_name|tag|icon|clear_display_name|clear_tag|clear_icon> [value]\n"
                "• Cor;serveridentitystatus (`sis`)\n"
                "• Cor;servermemberidentity (`smi`) <member_id> <display_name|tag|icon|clear_display_name|clear_tag|clear_icon> [subsystem_id] [value]\n"
                "• Cor;servermemberidentitystatus (`smis`) <member_id> [subsystem_id]\n\n"
                "**Privacy**\n"
                "• Cor;systemprivacy (`spv`) <private|trusted|public>\n"
                "• Cor;alterprivacy (`apv`) <member_id> <private|trusted|public> [subsystem_id]\n"
                "• Cor;privacystatus (`pvs`)\n\n"
                "**Timezone**\n"
                "• Cor;settimezone (`stz`) <timezone>\n"
                "• Cor;timezonestatus (`tzs`)"
            ),
            "color": discord.Color.dark_teal(),
        },
        {
            "title": "Cortex Help - Focus & Wellness",
            "description": (
                "**Focus Modes** *(singlet only)*\n"
                "• Cor;setmode (`sm`) <mode_name|clear>\n"
                "• Cor;currentmode (`cm`)\n"
                "• Cor;modestats (`mst`) [days]\n\n"
                "**Wellness**\n"
                "• Cor;checkin <rating> <energy> [notes]\n"
                "• Cor;checkinstatus\n"
                "• Cor;weeklymoodsummary <true|false>\n\n"
                "**Future Messaging** *(singlet only)*\n"
                "• Cor;sendmessage (`schd`) target:future time:<time> message:\"<message>\"\n\n"
                "**Front Reminders**\n"
                "• Cor;frontreminders <true|false>\n"
                "• Cor;setfrontreminderhours <hours>\n"
                "• Cor;frontreminderstatus"
            ),
            "color": discord.Color.brand_green(),
        },
        {
            "title": "Cortex Help - Fronting & Proxy",
            "description": (
                "**Fronting**\n"
                "• Cor;switchmember (`sw`) <member_id> [subsystem_id]\n"
                "• Cor;cofrontmember <member_id> [subsystem_id]\n"
                "• Cor;setstatus [status] [member_id] [subsystem_id]\n"
                "• Cor;currentfronts (`cf`) [all|main|subsystem_id]\n"
                "• Cor;fronthistory [subsystem_id]\n"
                "• Cor;frontstats [subsystem_id]\n"
                "• Cor;switchpatterns [subsystem_id]\n"
                "• Cor;memberstats <member_id> [subsystem_id]\n"
                "• Cor;memberhistory <member_id> [subsystem_id]\n"
                "• Cor;clearfront [subsystem_id]\n\n"
                "**Autoproxy**\n"
                "• Cor;globalautoproxy (`gap`) <off|front|latch>\n"
                "• Cor;autoproxy (`ap`) <inherit|off|front|latch>\n"
                "• Cor;autoproxystatus (`apst`)"
            ),
            "color": discord.Color.green(),
        },
        {
            "title": "Cortex Help - Members & Tags",
            "description": (
                "**Members**\n"
                "• Cor;addmember <fields...>\n"
                "• Cor;members (`mem`) [main|all|subsystem_id] [page] [target_user_id]\n"
                "• Cor;viewmember (`vm`) <member_id> [subsystem_id] [target_user_id]\n"
                "• Cor;random [target_user_id]\n"
                "• Cor;searchmember <query> [subsystem_id]\n"
                "• Cor;editmember <fields...>\n"
                "• Cor;editmemberimages <member_id> [subsystem_id]\n"
                "• Cor;editmembertag (`emt`) <member_id> <format_with_text|clear> [subsystem_id]\n"
                "• Cor;addmembertag (`amt`) <member_id> <format_with_text> [subsystem_id]\n"
                "• Cor;removemembertag (`rmt`) <member_id> <format_with_text> [subsystem_id]\n"
                "• Cor;membersort (`ms`) <id|alphabetical>\n"
                "• Cor;movemember (`mvm`) <member_id> <to_subsystem_id|main> [from_subsystem_id]\n"
                "• Cor;messageto (`msg`) <member_id> <message>\n\n"
                "**Tags**\n"
                "• Cor;addtag <tag>\n"
                "• Cor;listtags\n"
                "• Cor;removetag <tag>\n"
                "• Cor;browsetags [subsystem_id]"
            ),
            "color": discord.Color.gold(),
        },
        {
            "title": "Cortex Help - Groups",
            "description": (
                "**Groups**\n"
                "• Cor;creategroup (`cg`) <name> [parent_group_id]\n"
                "• Cor;editgroup (`eg`) <group_id> <name|parent> <value>\n"
                "• Cor;deletegroup (`dg`) <group_id> [true|false]\n"
                "• Cor;listgroups (`lg`)\n"
                "• Cor;grouporder (`go`) <id1,id2,id3,...>\n"
                "• Cor;grouporderui (`goui`)\n"
                "• Cor;addmembergroup (`amg`) <member_id> <group_id> [subsystem_id]\n"
                "• Cor;removemembergroup (`rmg`) <member_id> <group_id> [subsystem_id]\n"
                "• Cor;membergroups (`mg`) <member_id> [subsystem_id]"
            ),
            "color": discord.Color.dark_gold(),
        },
        {
            "title": "Cortex Help - External Messaging",
            "description": (
                "**External Settings**\n"
                "• Cor;allowexternal (`aex`) <true|false>\n"
                "• Cor;externalprivacy (`epr`) <public|private_summary|dm_only>\n"
                "• Cor;externalstatus (`est`)\n"
                "• Cor;externallimits (`elim`) [max_chars] [target_cooldown_seconds]\n"
                "• Cor;externaltrustedonly (`eto`) <true|false>\n"
                "• Cor;externalquiethours (`eqh`) <true|false> [start_hour] [end_hour]\n"
                "• Cor;externalretention (`ert`) <days>\n"
                "• Cor;externalpending (`ep`)\n"
                "• Cor;approveexternal (`apx`) <user_id> [true|false]\n"
                "• Cor;recentexternal (`rex`) [limit]\n\n"
                "**Sending**\n"
                "• Cor;sendexternal (`sxe`) <target_user_id> <target_member_id> <message>"
            ),
            "color": discord.Color.orange(),
        },
        {
            "title": "Cortex Help - Safety & Moderation",
            "description": (
                "**Safety Lists**\n"
                "• Cor;blockuser (`bu`) / Cor;unblockuser (`ubu`) / Cor;blockedusers (`bus`)\n"
                "• Cor;trustuser (`tru`) / Cor;untrustuser (`utru`) / Cor;trustedusers (`trus`)\n"
                "• Cor;muteuser (`mu`) / Cor;unmuteuser (`umu`) / Cor;mutedusers (`mus`)\n"
                "• Cor;tempblockuser (`tbu`) <user_id> [hours]\n"
                "• Cor;tempblockedusers (`tbus`)\n"
                "• Cor;reportexternal (`rptx`) <user_id> <reason>\n\n"
                "**Moderator Commands**\n"
                "• Cor;modreports (`mr`) [limit]\n"
                "• Cor;modwarn (`mw`) <user_id> [reason]\n"
                "• Cor;modsuspend (`msu`) <user_id> [hours] [scope] [reason]\n"
                "• Cor;modban (`mb`) <user_id> [scope] [reason]\n"
                "• Cor;modunban (`mub`) <user_id> [clear_warnings]\n"
                "• Cor;modappeal (`map`) <message>"
            ),
            "color": discord.Color.red(),
        },
        {
            "title": "Cortex Help - Maintenance",
            "description": (
                "**Cleanup & Removal**\n"
                "• Cor;removemember <member_id> [subsystem_id]\n"
                "• Cor;removemembers <member_ids...> [subsystem_id]\n"
                "• Cor;clearsystem\n"
                "• Cor;clearsubsystem <subsystem_id> [move|delete]\n"
                "• Cor;deletesystem\n"
                "• Cor;clearall [subsystem_id]\n\n"
                "**Maintenance**\n"
                "• Cor;refresh\n"
                "• Cor;synccommands"
            ),
            "color": discord.Color.dark_red(),
        },
        {
            "title": "Cortex Help - Alias Quick Map",
            "description": (
                "`h` help | `reg` register | `css` createsubsystem | `es` editsubsystem | `vssc` viewsubsystemcard | `esc` editsubsystemcard | `lss` listsubsystems | `stag` systemtag | `spv` systemprivacy | `apv` alterprivacy | `pvs` privacystatus | `vsc` viewsystemcard\n"
                "`stz` settimezone | `tzs` timezonestatus | `sw` switchmember | `cf` currentfronts | `gap` globalautoproxy | `ap` autoproxy | `apst` autoproxystatus | `si` serveridentity | `sis` serveridentitystatus | `smi` servermemberidentity | `smis` servermemberidentitystatus\n"
                "`sm` setmode | `cm` currentmode | `mst` modestats | `schd` sendmessage\n"
                "`mem` members | `vm` viewmember | `emt` editmembertag | `amt` addmembertag | `rmt` removemembertag | `ms` membersort | `mvm` movemember | `msg` messageto\n"
                "`cg` creategroup | `eg` editgroup | `dg` deletegroup | `lg` listgroups | `go` grouporder | `goui` grouporderui | `amg` addmembergroup | `rmg` removemembergroup | `mg` membergroups\n"
                "`aex` allowexternal | `epr` externalprivacy | `est` externalstatus | `elim` externallimits | `eto` externaltrustedonly | `eqh` externalquiethours | `ert` externalretention | `sxe` sendexternal\n"
                "`bu` blockuser | `ubu` unblockuser | `bus` blockedusers | `tru` trustuser | `utru` untrustuser | `trus` trustedusers\n"
                "`mu` muteuser | `umu` unmuteuser | `mus` mutedusers | `tbu` tempblockuser | `tbus` tempblockedusers | `ep` externalpending | `apx` approveexternal | `rex` recentexternal | `rptx` reportexternal\n"
                "`mr` modreports | `mw` modwarn | `msu` modsuspend | `mb` modban | `mub` modunban | `map` modappeal"
            ),
            "color": discord.Color.light_grey(),
        },
    ]

    def build_embed(page_index: int) -> discord.Embed:
        page = pages[page_index]
        embed = discord.Embed(
            title=page["title"],
            description=page["description"],
            color=page["color"],
        )
        embed.set_footer(text=f"Page {page_index + 1}/{len(pages)}")
        return embed

    class HelpPaginator(discord.ui.View):
        def __init__(self, owner_id: int):
            super().__init__(timeout=180)
            self.owner_id = owner_id
            self.page_index = 0
            self._update_button_state()

        def _update_button_state(self):
            self.prev_button.disabled = self.page_index <= 0
            self.next_button.disabled = self.page_index >= len(pages) - 1
            for option in self.jump_to_page.options:
                option.default = option.value == str(self.page_index)
            self.jump_to_page.placeholder = f"Jump to page ({self.page_index + 1}/{len(pages)})"

        async def interaction_check(self, interaction: discord.Interaction) -> bool:
            if interaction.user.id != self.owner_id:
                await interaction.response.send_message("Only the command author can use these buttons.", ephemeral=True)
                return False
            return True

        async def on_timeout(self):
            self.prev_button.disabled = True
            self.next_button.disabled = True
            self.jump_to_page.disabled = True
            self.close_button.disabled = True

        @discord.ui.select(
            placeholder="Jump to page",
            min_values=1,
            max_values=1,
            options=[
                discord.SelectOption(label="1) Setup & Cards", value="0"),
                discord.SelectOption(label="2) Identity & Privacy", value="1"),
                discord.SelectOption(label="3) Focus & Wellness", value="2"),
                discord.SelectOption(label="4) Fronting & Proxy", value="3"),
                discord.SelectOption(label="5) Members & Tags", value="4"),
                discord.SelectOption(label="6) Groups", value="5"),
                discord.SelectOption(label="7) External Messaging", value="6"),
                discord.SelectOption(label="8) Safety & Moderation", value="7"),
                discord.SelectOption(label="9) Maintenance", value="8"),
                discord.SelectOption(label="10) Alias Quick Map", value="9"),
            ],
        )
        async def jump_to_page(self, interaction: discord.Interaction, select: discord.ui.Select):
            self.page_index = int(select.values[0])
            self._update_button_state()
            await interaction.response.edit_message(embed=build_embed(self.page_index), view=self)

        @discord.ui.button(label="Previous", style=discord.ButtonStyle.secondary)
        async def prev_button(self, interaction: discord.Interaction, button: discord.ui.Button):
            if self.page_index > 0:
                self.page_index -= 1
            self._update_button_state()
            await interaction.response.edit_message(embed=build_embed(self.page_index), view=self)

        @discord.ui.button(label="Next", style=discord.ButtonStyle.primary)
        async def next_button(self, interaction: discord.Interaction, button: discord.ui.Button):
            if self.page_index < len(pages) - 1:
                self.page_index += 1
            self._update_button_state()
            await interaction.response.edit_message(embed=build_embed(self.page_index), view=self)

        @discord.ui.button(label="Close", style=discord.ButtonStyle.danger)
        async def close_button(self, interaction: discord.Interaction, button: discord.ui.Button):
            await interaction.response.edit_message(view=None)
            self.stop()

    view = HelpPaginator(ctx.author.id)
    await ctx.send(embed=build_embed(0), view=view)


@bot.command(name="viewsystemcard", aliases=["vsc"])
async def viewsystemcard_prefix(ctx: commands.Context, target_user_id: str = None):
    requester_id = ctx.author.id
    system_id, system, target_owner_id, error = resolve_target_system_for_view(requester_id, target_user_id)
    if error:
        await ctx.send(error)
        return

    if str(target_owner_id) != str(requester_id) and not can_view_system_data(system, requester_id):
        await ctx.send("You do not have permission to view this system card.")
        return

    profile = get_system_profile(system)
    try:
        embed_color = int(str(profile.get("color", "00DE9B")).lstrip("#"), 16)
    except (TypeError, ValueError):
        embed_color = int("00DE9B", 16)

    embed = discord.Embed(
        title=f"{system.get('system_name', 'Unnamed System')} - System Card",
        description=profile.get("description") or "No description set.",
        color=embed_color
    )
    embed.add_field(name="Mode", value=system.get("mode", "system").title(), inline=True)
    embed.add_field(name="Collective Pronouns", value=profile.get("collective_pronouns") or "Not set", inline=True)
    embed.add_field(name="System Tag", value=get_system_proxy_tag(system) or "Not set", inline=True)

    if profile.get("profile_pic"):
        embed.set_thumbnail(url=profile["profile_pic"])
    if profile.get("banner"):
        embed.set_image(url=profile["banner"])

    await ctx.send(embed=embed)


@bot.command(name="timezonestatus", aliases=["tzs"])
async def timezonestatus_prefix(ctx: commands.Context):
    user_id = ctx.author.id
    system_id = get_user_system_id(user_id)
    if not system_id:
        await ctx.send("You must register a main system first using /register.")
        return

    system = systems_data["systems"].get(system_id)
    if not system:
        await ctx.send("System not found.")
        return

    await ctx.send(f"Current timezone: **{get_system_timezone_name(system)}**")


@bot.command(name="settimezone", aliases=["stz"])
async def settimezone_prefix(ctx: commands.Context, *, timezone_name: str = None):
    user_id = ctx.author.id
    system_id = get_user_system_id(user_id)
    if not system_id:
        await ctx.send("You must register a main system first using /register.")
        return

    normalized = normalize_timezone_name(timezone_name)
    if not normalized:
        PENDING_TIMEZONE_PROMPTS[ctx.author.id] = datetime.now(timezone.utc) + timedelta(minutes=2)
        await ctx.send(
            "Please provide a timezone name. Reply with just the timezone (for example: EST, UTC, America/New_York)."
        )
        return

    try:
        _ = ZoneInfo(normalized)
    except ZoneInfoNotFoundError:
        if normalized not in TIMEZONE_FIXED_OFFSETS:
            await ctx.send(
                "Unknown timezone. Try a value like `America/New_York`, `UTC`, or `Asia/Tokyo`."
            )
            return

    system = systems_data["systems"].get(system_id)
    if not system:
        await ctx.send("System not found.")
        return

    system["timezone"] = normalized
    save_systems()
    await ctx.send(f"Timezone set to **{normalized}**.")


# -----------------------------
# Focus Mode prefix commands (singlet only)
# -----------------------------

@bot.command(name="setmode", aliases=["sm"])
async def setmode_prefix(ctx: commands.Context, *, args: str = None):
    user_id = ctx.author.id
    system_id = get_user_system_id(user_id)
    if not system_id:
        await ctx.send("You must register a profile first using /register.")
        return
    system = systems_data["systems"].get(system_id)
    if not system:
        await ctx.send("System not found.")
        return
    if system.get("mode") != "singlet":
        await ctx.send("Focus modes are only available for singlet profiles.")
        return

    if not args:
        await ctx.send(
            "Usage: `Cor;setmode <mode_name>` or `Cor;setmode clear`\n"
            f"Default modes: {', '.join(m.title() for m in DEFAULT_FOCUS_MODES)}"
        )
        return

    token = args.strip().lower()
    if token == "clear":
        ended = end_focus_mode(system)
        save_systems()
        if ended:
            await ctx.send("Focus mode cleared.")
        else:
            await ctx.send("No active focus mode to clear.")
        return

    all_modes = get_all_mode_names(system)
    fm = get_focus_modes(system)
    if token not in all_modes:
        fm.setdefault("custom_modes", []).append(token)

    start_focus_mode(system, token)
    save_systems()
    await ctx.send(f"Focus mode set to **{token.title()}**.")


@bot.command(name="currentmode", aliases=["cm"])
async def currentmode_prefix(ctx: commands.Context):
    user_id = ctx.author.id
    system_id = get_user_system_id(user_id)
    if not system_id:
        await ctx.send("You must register a profile first using /register.")
        return
    system = systems_data["systems"].get(system_id)
    if not system:
        await ctx.send("System not found.")
        return
    if system.get("mode") != "singlet":
        await ctx.send("Focus modes are only available for singlet profiles.")
        return

    fm = get_focus_modes(system)
    cur = fm.get("current")
    if not cur:
        await ctx.send("No active focus mode. Use `Cor;setmode <mode>` to set one.")
        return

    started_dt = datetime.fromisoformat(cur["started"])
    elapsed = (datetime.now(timezone.utc) - started_dt).total_seconds()
    dur = format_duration(elapsed)
    await ctx.send(f"Current mode: **{cur['name'].title()}** | Active for: **{dur}**")


@bot.command(name="modestats", aliases=["mst"])
async def modestats_prefix(ctx: commands.Context, days: str = "30"):
    user_id = ctx.author.id
    system_id = get_user_system_id(user_id)
    if not system_id:
        await ctx.send("You must register a profile first using /register.")
        return
    system = systems_data["systems"].get(system_id)
    if not system:
        await ctx.send("System not found.")
        return
    if system.get("mode") != "singlet":
        await ctx.send("Focus modes are only available for singlet profiles.")
        return

    try:
        days_int = int(days)
    except ValueError:
        await ctx.send("Days must be a number. Example: `Cor;modestats 30`")
        return

    if days_int < 1 or days_int > 365:
        await ctx.send("Days must be between 1 and 365.")
        return

    totals = calc_mode_stats(system, days=days_int)
    if not totals:
        await ctx.send(f"No focus mode data for the last **{days_int}** day(s).")
        return

    sorted_modes = sorted(totals.items(), key=lambda x: x[1], reverse=True)
    total_all = sum(totals.values())
    lines = [f"**Focus Mode Stats — Last {days_int} Day(s)**"]
    for name, secs in sorted_modes:
        pct = (secs / total_all * 100) if total_all else 0
        lines.append(f"• **{name.title()}** — {format_duration(secs)} ({pct:.0f}%)")
    lines.append(f"\nTotal tracked: {format_duration(total_all)}")
    await ctx.send("\n".join(lines))


@bot.command(name="allowexternal", aliases=["aex"])
async def allowexternal_prefix(ctx: commands.Context, enabled: str):
    parsed = parse_bool_token(enabled)
    if parsed is None:
        await ctx.send("Invalid value. Use true/false.")
        return

    user_id = ctx.author.id
    system_id = get_user_system_id(user_id)
    if not system_id:
        await ctx.send("You must register using /register.")
        return

    system = systems_data["systems"].get(system_id)
    if not system:
        await ctx.send("System not found.")
        return

    settings = get_external_settings(system)
    settings["accept"] = parsed
    save_systems()
    status = "enabled" if parsed else "disabled"
    await ctx.send(f"External messages are now **{status}**.")


@bot.command(name="switchmember", aliases=["sw"])
async def switchmember_prefix(ctx: commands.Context, member_id: str, subsystem_id: str = None):
    user_id = ctx.author.id
    system_id = get_user_system_id(user_id)
    if not system_id:
        await ctx.send("You must register a main system first using /register.")
        return

    system = systems_data.get("systems", {}).get(system_id, {})
    target_scope_id, members, resolved_member_id, resolved_member, resolve_error = resolve_member_identifier_in_system(
        system,
        member_id,
        subsystem_id=subsystem_id,
    )
    if resolve_error:
        await ctx.send(resolve_error)
        return

    for m in members.values():
        if m.get("current_front"):
            end_front(m["id"], members_dict=members)

    start_front(resolved_member_id, members_dict=members)

    response = f"Member **{resolved_member.get('name', resolved_member_id)}** is now fronting in {get_scope_label(target_scope_id)}."
    if cleanup_external_inbox_entries(system):
        save_systems()

    inbox = resolved_member.get("inbox", [])
    if inbox:
        external_settings = get_external_settings(system)
        delivery_mode = external_settings.get("delivery_mode", "public")

        external_msgs = [m for m in inbox if m.get("external")]
        internal_msgs = [m for m in inbox if not m.get("external")]

        response += f"\n\n📨 **You have {len(inbox)} message(s):**"

        for msg in internal_msgs:
            response += f"\n{format_inbox_entry_for_channel(msg)}"

        if external_msgs and delivery_mode == "public":
            for msg in external_msgs:
                response += f"\n{format_inbox_entry_for_channel(msg)}"
        elif external_msgs and delivery_mode == "private_summary":
            response += f"\n> You have **{len(external_msgs)} external** message(s). Details sent by DM."
        elif external_msgs and delivery_mode == "dm_only":
            response += "\n> You have new external messages. Details sent by DM."

        if external_msgs and delivery_mode in {"private_summary", "dm_only"}:
            try:
                if is_in_quiet_hours(system):
                    response += "\n> Quiet hours are active, so external details were not DM'd right now."
                else:
                    dm_lines = [
                        f"External inbox for {resolved_member.get('name', resolved_member_id)} ({get_scope_label(target_scope_id)}):"
                    ]
                    for idx, msg in enumerate(external_msgs, start=1):
                        dm_lines.append(f"\n#{idx}\n{format_inbox_entry_for_dm(msg)}")
                    await ctx.author.send("\n".join(dm_lines))
            except (discord.Forbidden, discord.HTTPException):
                response += "\n> I could not DM you. External message details are hidden due to your privacy mode."

        resolved_member["inbox"] = []

    save_systems()
    await ctx.send(response)


@bot.command(name="cofrontmember", aliases=["cfm"])
async def cofrontmember_prefix(
    ctx: commands.Context,
    member_id: str = None,
    subsystem_id: str = None,
    *,
    cofront_member_ids: str = None,
):
    if not member_id:
        await ctx.send("Usage: Cor;cofrontmember <member_id_or_name> [subsystem_id] [cofront_ids_or_names]")
        return

    user_id = ctx.author.id
    system_id = get_user_system_id(user_id)
    if not system_id:
        await ctx.send("You must register a main system first using /register.")
        return

    # Support shorthand: Cor;cofrontmember <member_id> <cofront_ids>
    # If second arg is not a valid scope, treat it as co-front IDs in main scope.
    members = get_system_members(system_id, subsystem_id)
    if members is None and subsystem_id and not cofront_member_ids:
        cofront_member_ids = subsystem_id
        subsystem_id = None
        members = get_system_members(system_id, subsystem_id)

    if members is None:
        await ctx.send(f"Member not found in {get_scope_label(subsystem_id)}.")
        return

    resolved_member_id, resolved_member, resolve_error = resolve_member_identifier(members, member_id)
    if resolve_error:
        await ctx.send(resolve_error)
        return

    selected_cofronts = []
    if cofront_member_ids:
        tokens = [
            t.strip()
            for t in cofront_member_ids.replace(";", ",").replace(" ", ",").split(",")
            if t.strip()
        ]
        seen = set()
        for token in tokens:
            resolved_co_id, _resolved_co_member, co_err = resolve_member_identifier(members, token)
            if co_err:
                await ctx.send(f"Co-front member `{token}` could not be resolved: {co_err}")
                return
            if resolved_co_id == resolved_member_id:
                await ctx.send("Primary member cannot also be listed as a co-front.")
                return
            if resolved_co_id not in seen:
                seen.add(resolved_co_id)
                selected_cofronts.append(resolved_co_id)

    for m in members.values():
        if m.get("current_front"):
            end_front(m["id"], members_dict=members)

    start_front(resolved_member_id, cofronts=selected_cofronts, members_dict=members)
    save_systems()

    co_names = ", ".join([members[c]["name"] for c in selected_cofronts]) if selected_cofronts else "None"
    await ctx.send(
        f"Member **{resolved_member.get('name', resolved_member_id)}** is now fronting with co-fronts: {co_names} in {get_scope_label(subsystem_id)}."
    )


@bot.command(name="currentfronts", aliases=["cf"])
async def currentfronts_prefix(ctx: commands.Context, scope: str = "all"):
    user_id = ctx.author.id
    system_id = get_user_system_id(user_id)
    if not system_id:
        await ctx.send("You must register a main system first using /register.")
        return

    system = systems_data.get("systems", {}).get(system_id)
    if not system:
        await ctx.send("System not found.")
        return

    scope_lower = (scope or "all").strip().lower()
    if scope_lower == "all":
        scope_sets = list(iter_system_member_dicts(system))
        title = "Current Fronts - Entire System"
    elif scope_lower in {"main", "none"}:
        members_dict = get_system_members(system_id, None)
        scope_sets = [(None, members_dict)]
        title = "Current Fronts - Main System"
    else:
        members_dict = get_system_members(system_id, scope)
        if members_dict is None:
            await ctx.send("Subsystem not found.")
            return
        scope_sets = [(scope, members_dict)]
        title = f"Current Fronts - {get_scope_label(scope).capitalize()}"

    fronters = []
    for scope_id, scoped_members in scope_sets:
        scope_label = get_scope_label(scope_id)
        for m in scoped_members.values():
            current = m.get("current_front")
            if not current:
                continue

            start_dt = datetime.fromisoformat(current["start"])
            duration = format_duration((datetime.now(timezone.utc) - start_dt).total_seconds())
            cofront_names = [scoped_members[c]["name"] for c in current.get("cofronts", []) if c in scoped_members]
            cofront_str = f" - Co-fronting: {', '.join(cofront_names)}" if cofront_names else ""
            status_text = (current.get("status") or "").strip()
            status_str = f" - Status: {status_text}" if status_text else ""
            fronters.append(f"- {m['name']} ({scope_label}) - {duration}{status_str}{cofront_str}")

    embed = discord.Embed(
        title=title,
        description="\n".join(fronters) if fronters else "No members are currently fronting.",
        color=discord.Color.purple()
    )
    await ctx.send(embed=embed)


@bot.command(name="globalautoproxy", aliases=["gap"])
async def globalautoproxy_prefix(ctx: commands.Context, mode: str = None):
    if mode is None:
        await ctx.send("Usage: Cor;globalautoproxy <off|front|latch>")
        return

    normalized_mode = (mode or "").strip().lower()
    if normalized_mode not in {"off", "front", "latch"}:
        await ctx.send("Invalid mode. Use `off`, `front`, or `latch`.")
        return

    user_id = ctx.author.id
    system_id = get_user_system_id(user_id)
    if not system_id:
        await ctx.send("You must register a main system first using /register.")
        return

    system = systems_data.get("systems", {}).get(system_id)
    if not system:
        await ctx.send("System not found.")
        return

    settings = get_autoproxy_settings(system)
    apply_autoproxy_mode(settings, normalized_mode)
    save_systems()

    await ctx.send(f"Global autoproxy set to **{normalized_mode}**.")


@bot.command(name="autoproxy", aliases=["ap"])
async def autoproxy_prefix(ctx: commands.Context, mode: str = None):
    if mode is None:
        await ctx.send("Usage: Cor;autoproxy <inherit|off|front|latch>")
        return

    if ctx.guild is None:
        await ctx.send("This command can only be used in a server.")
        return

    normalized_mode = (mode or "").strip().lower()
    if normalized_mode not in {"inherit", "off", "front", "latch"}:
        await ctx.send("Invalid mode. Use `inherit`, `off`, `front`, or `latch`.")
        return

    user_id = ctx.author.id
    system_id = get_user_system_id(user_id)
    if not system_id:
        await ctx.send("You must register a main system first using /register.")
        return

    system = systems_data.get("systems", {}).get(system_id)
    if not system:
        await ctx.send("System not found.")
        return

    overrides = system.setdefault("autoproxy_server_overrides", {})
    guild_key = str(ctx.guild.id)

    if normalized_mode == "inherit":
        overrides.pop(guild_key, None)
        save_systems()
        await ctx.send("Server autoproxy now inherits your global autoproxy setting.")
        return

    settings = get_server_autoproxy_settings(system, ctx.guild.id, create=True)
    apply_autoproxy_mode(settings, normalized_mode)
    save_systems()

    await ctx.send(f"Server autoproxy set to **{normalized_mode}** for this server.")


@bot.command(name="autoproxystatus", aliases=["apst"])
async def autoproxystatus_prefix(ctx: commands.Context):
    user_id = ctx.author.id
    system_id = get_user_system_id(user_id)
    if not system_id:
        await ctx.send("You must register a main system first using /register.")
        return

    system = systems_data.get("systems", {}).get(system_id)
    if not system:
        await ctx.send("System not found.")
        return

    global_settings = get_autoproxy_settings(system)
    lines = [f"Global mode: **{global_settings.get('mode', 'off')}**"]

    if ctx.guild is None:
        lines.append("Server mode: N/A (DM context)")
        lines.append(f"Effective mode: **{global_settings.get('mode', 'off')}**")
    else:
        server_settings = get_server_autoproxy_settings(system, ctx.guild.id, create=False)
        if server_settings is None:
            lines.append("Server mode: **inherit global**")
            lines.append(f"Effective mode: **{global_settings.get('mode', 'off')}**")
        else:
            lines.append(f"Server mode: **{server_settings.get('mode', 'off')}**")
            lines.append(f"Effective mode: **{server_settings.get('mode', 'off')}**")

    await ctx.send("\n".join(lines))


@bot.command(name="members", aliases=["mem"])
async def members_prefix(ctx: commands.Context, scope: str = "main", page: int = 1, target_user_id: str = None):
    requester_id = ctx.author.id
    system_id, system, target_owner_id, error = resolve_target_system_for_view(requester_id, target_user_id)
    if error:
        await ctx.send(error)
        return

    if str(target_owner_id) != str(requester_id) and not can_view_system_data(system, requester_id):
        await ctx.send("You do not have permission to view this member list.")
        return

    scope_lower = (scope or "main").strip().lower()
    if scope_lower == "all":
        member_rows = []
        scoped_members_lookup = {}
        for scope_id, scoped_members in iter_system_member_dicts(system):
            scoped_members_lookup[scope_id] = scoped_members
            for member_id, member in scoped_members.items():
                if str(target_owner_id) != str(requester_id) and not can_view_member_data(system, member, requester_id):
                    continue
                member_rows.append((scope_id, member_id, member))
        title_scope = "Entire System"
    elif scope_lower in {"main", "none"}:
        members_dict = get_system_members(system_id, None)
        scoped_members_lookup = {None: members_dict}
        member_rows = []
        for member_id, member in members_dict.items():
            if str(target_owner_id) != str(requester_id) and not can_view_member_data(system, member, requester_id):
                continue
            member_rows.append((None, member_id, member))
        title_scope = "Main System"
    else:
        members_dict = get_system_members(system_id, scope)
        if members_dict is None:
            await ctx.send("Subsystem not found.")
            return
        scoped_members_lookup = {scope: members_dict}
        member_rows = []
        for member_id, member in members_dict.items():
            if str(target_owner_id) != str(requester_id) and not can_view_member_data(system, member, requester_id):
                continue
            member_rows.append((scope, member_id, member))
        title_scope = get_scope_label(scope).capitalize()

    if not member_rows:
        await ctx.send("No visible members found.")
        return

    sort_mode = get_member_sort_mode(system)
    member_rows = sort_member_rows(member_rows, sort_mode)

    members_per_page = 15
    total_pages = (len(member_rows) - 1) // members_per_page + 1 if member_rows else 1
    start_page = max(1, min(page, total_pages))

    def get_embed(page_num):
        start_idx = (page_num - 1) * members_per_page
        end_idx = start_idx + members_per_page
        page_members = member_rows[start_idx:end_idx]

        desc_lines = []
        for scope_id, member_id, member in page_members:
            current_front = member.get("current_front")
            fronting = "Yes" if current_front else "No"
            duration = format_duration(calculate_front_duration(member))
            scoped_members = scoped_members_lookup.get(scope_id, {})
            cofront_ids = current_front.get("cofronts", []) if current_front else []
            cofront_names = [scoped_members.get(co_id, {}).get("name", str(co_id)) for co_id in cofront_ids]
            co_fronts = ", ".join(cofront_names) if cofront_names else "None"
            scope_label = get_scope_label(scope_id)
            desc_lines.append(
                f"**{member['name']}** (ID `{member_id}`) | Scope: {scope_label} | Fronting: {fronting} | Co-fronts: {co_fronts} | Total Front Time: {duration}"
            )

        embed = discord.Embed(
            title=f"Members List - {title_scope} (Page {page_num}/{total_pages})",
            description="\n".join(desc_lines) if desc_lines else "No members found.",
            color=discord.Color.green(),
        )
        embed.set_footer(text=f"Sort: {'Alphabetical' if sort_mode == 'alphabetical' else 'ID'}")
        return embed

    class PrefixMembersPaginator(discord.ui.View):
        def __init__(self, owner_id, current_page):
            super().__init__(timeout=120)
            self.owner_id = owner_id
            self.current_page = current_page

        async def interaction_check(self, interaction: discord.Interaction) -> bool:
            if interaction.user.id != self.owner_id:
                await interaction.response.send_message("Only the command author can use these buttons.", ephemeral=True)
                return False
            return True

        @discord.ui.button(label="Previous", style=discord.ButtonStyle.primary)
        async def prev(self, interaction: discord.Interaction, button: discord.ui.Button):
            if self.current_page > 1:
                self.current_page -= 1
            await interaction.response.edit_message(embed=get_embed(self.current_page), view=self)

        @discord.ui.button(label="Next", style=discord.ButtonStyle.primary)
        async def next(self, interaction: discord.Interaction, button: discord.ui.Button):
            if self.current_page < total_pages:
                self.current_page += 1
            await interaction.response.edit_message(embed=get_embed(self.current_page), view=self)

    view = PrefixMembersPaginator(ctx.author.id, start_page)
    await ctx.send(embed=get_embed(start_page), view=view)


@bot.command(name="viewmember", aliases=["vm"])
async def viewmember_prefix(ctx: commands.Context, member_id: str = None, subsystem_id: str = None, target_user_id: str = None):
    if not member_id:
        await ctx.send("Usage: Cor;viewmember <member_id or name> [subsystem_id] [target_user_id]")
        return

    requester_id = ctx.author.id
    system_id, system, target_owner_id, error = resolve_target_system_for_view(requester_id, target_user_id)
    if error:
        await ctx.send(error)
        return

    target_scope_id, members_dict, resolved_member_id, member, resolve_error = \
        resolve_member_identifier_in_system(system, member_id, subsystem_id=subsystem_id)
    if resolve_error:
        await ctx.send(resolve_error)
        return

    if str(target_owner_id) != str(requester_id) and not can_view_member_data(system, member, requester_id):
        await ctx.send("You do not have permission to view this member card.")
        return

    await ctx.send(embed=build_member_profile_embed(member, system=system))


@bot.command(name="editmemberimages", aliases=["emi"])
async def editmemberimages_prefix(ctx: commands.Context, member_id: str = None, subsystem_id: str = None):
    if not member_id:
        await ctx.send("Usage: Cor;editmemberimages <member_id> [subsystem_id] (attach 1-2 image files)")
        return

    user_id = ctx.author.id
    system_id = get_user_system_id(user_id)
    if not system_id:
        await ctx.send("You must register using /register.")
        return

    members_dict = get_system_members(system_id, subsystem_id)
    if members_dict is None or member_id not in members_dict:
        await ctx.send(f"Member not found in {get_scope_label(subsystem_id)}.")
        return

    attachments = list(ctx.message.attachments or [])
    if not attachments:
        await ctx.send("Attach 1 image (profile or banner) or 2 images (profile first, banner second).")
        return

    member = members_dict[member_id]
    updated = []

    if len(attachments) >= 2:
        member["profile_pic"] = attachments[0].url
        member["banner"] = attachments[1].url
        updated = ["profile_pic", "banner"]
    else:
        single = attachments[0]
        filename = (single.filename or "").lower()
        if "banner" in filename:
            member["banner"] = single.url
            updated = ["banner"]
        else:
            member["profile_pic"] = single.url
            updated = ["profile_pic"]

    save_systems()
    await ctx.send(
        f"Updated **{', '.join(updated)}** for **{member.get('name', member_id)}** in {get_scope_label(subsystem_id)}."
    )


@bot.command(name="memberimagedebug", aliases=["midbg"])
async def memberimagedebug_prefix(ctx: commands.Context, member_id: str = None, subsystem_id: str = None, target_user_id: str = None):
    if not member_id:
        await ctx.send("Usage: Cor;memberimagedebug <member_id> [subsystem_id] [target_user_id]")
        return

    requester_id = ctx.author.id
    system_id, system, target_owner_id, error = resolve_target_system_for_view(requester_id, target_user_id)
    if error:
        await ctx.send(error)
        return

    members_dict = get_system_members(system_id, subsystem_id)
    if members_dict is None or member_id not in members_dict:
        await ctx.send(f"Member not found in {get_scope_label(subsystem_id)}.")
        return

    member = members_dict[member_id]
    if str(target_owner_id) != str(requester_id) and not can_view_member_data(system, member, requester_id):
        await ctx.send("You do not have permission to view this member card.")
        return

    raw_pic = str(member.get("profile_pic") or "")
    raw_banner = str(member.get("banner") or "")
    norm_pic = normalize_embed_image_url(raw_pic)
    norm_banner = normalize_embed_image_url(raw_banner)

    lines = [
        f"Member: **{member.get('name', member_id)}** (`{member_id}`)",
        f"Scope: {get_scope_label(subsystem_id)}",
        f"Raw profile_pic set: {'yes' if raw_pic.strip() else 'no'}",
        f"Raw banner set: {'yes' if raw_banner.strip() else 'no'}",
        f"Normalized profile_pic valid: {'yes' if norm_pic else 'no'}",
        f"Normalized banner valid: {'yes' if norm_banner else 'no'}",
    ]

    if norm_pic:
        lines.append(f"Profile URL: {norm_pic}")
    if norm_banner:
        lines.append(f"Banner URL: {norm_banner}")

    await ctx.send("\n".join(lines))


@bot.command(name="editmembertag", aliases=["emt"])
async def editmembertag_prefix(
    ctx: commands.Context,
    member_id: str = None,
    proxy_format: str = None,
    subsystem_id: str = None,
):
    if not member_id or proxy_format is None:
        await ctx.send("Usage: Cor;editmembertag <member_id> <format_with_text|clear> [subsystem_id]")
        return

    user_id = ctx.author.id
    system_id = get_user_system_id(user_id)
    if not system_id:
        await ctx.send("You must register a main system first using /register.")
        return

    members_dict = get_system_members(system_id, subsystem_id)
    if members_dict is None or member_id not in members_dict:
        await ctx.send(f"Member not found in {get_scope_label(subsystem_id)}.")
        return

    member = members_dict[member_id]
    raw = proxy_format.strip()
    if raw.lower() in {"clear", "none", "null", "off", "-"}:
        set_member_proxy_formats(member, [])
        save_systems()
        await ctx.send(f"Cleared proxy tag for **{member.get('name', member_id)}** in {get_scope_label(subsystem_id)}.")
        return

    prefix, suffix, err = parse_proxy_format_with_placeholder(raw)
    if err == "missing_placeholder":
        await ctx.send("Proxy format must include `text` once. Example: `[text]` or `text>>`.")
        return
    if err == "multiple_placeholders":
        await ctx.send("Proxy format can only include `text` once.")
        return
    if err == "only_placeholder":
        await ctx.send("Proxy format cannot be just `text`; add a prefix or suffix.")
        return
    if err == "empty":
        await ctx.send("Proxy format cannot be empty.")
        return

    set_member_proxy_formats(member, [{"prefix": prefix, "suffix": suffix}])
    save_systems()
    await ctx.send(
        f"Set proxy tag for **{member.get('name', member_id)}** in {get_scope_label(subsystem_id)} to {render_member_proxy_result(member)}."
    )


@bot.command(name="addmembertag", aliases=["amt"])
async def addmembertag_prefix(
    ctx: commands.Context,
    member_id: str = None,
    proxy_format: str = None,
    subsystem_id: str = None,
):
    if not member_id or proxy_format is None:
        await ctx.send("Usage: Cor;addmembertag <member_id> <format_with_text> [subsystem_id]")
        return

    user_id = ctx.author.id
    system_id = get_user_system_id(user_id)
    if not system_id:
        await ctx.send("You must register a main system first using /register.")
        return

    members_dict = get_system_members(system_id, subsystem_id)
    if members_dict is None or member_id not in members_dict:
        await ctx.send(f"Member not found in {get_scope_label(subsystem_id)}.")
        return

    prefix, suffix, err = parse_proxy_format_with_placeholder(proxy_format.strip())
    if err == "missing_placeholder":
        await ctx.send("Proxy format must include `text` once. Example: `[text]` or `text>>`.")
        return
    if err == "multiple_placeholders":
        await ctx.send("Proxy format can only include `text` once.")
        return
    if err == "only_placeholder":
        await ctx.send("Proxy format cannot be just `text`; add a prefix or suffix.")
        return
    if err == "empty":
        await ctx.send("Proxy format cannot be empty.")
        return

    member = members_dict[member_id]
    if not add_member_proxy_format(member, prefix, suffix):
        await ctx.send("That proxy format is already on this member.")
        return

    save_systems()
    await ctx.send(
        f"Added proxy format for **{member.get('name', member_id)}** in {get_scope_label(subsystem_id)}.\nCurrent formats: {render_member_proxy_format(member)}"
    )


@bot.command(name="removemembertag", aliases=["rmt"])
async def removemembertag_prefix(
    ctx: commands.Context,
    member_id: str = None,
    proxy_format: str = None,
    subsystem_id: str = None,
):
    if not member_id or proxy_format is None:
        await ctx.send("Usage: Cor;removemembertag <member_id> <format_with_text> [subsystem_id]")
        return

    user_id = ctx.author.id
    system_id = get_user_system_id(user_id)
    if not system_id:
        await ctx.send("You must register a main system first using /register.")
        return

    members_dict = get_system_members(system_id, subsystem_id)
    if members_dict is None or member_id not in members_dict:
        await ctx.send(f"Member not found in {get_scope_label(subsystem_id)}.")
        return

    prefix, suffix, err = parse_proxy_format_with_placeholder(proxy_format.strip())
    if err == "missing_placeholder":
        await ctx.send("Proxy format must include `text` once. Example: `[text]` or `text>>`.")
        return
    if err == "multiple_placeholders":
        await ctx.send("Proxy format can only include `text` once.")
        return
    if err == "only_placeholder":
        await ctx.send("Proxy format cannot be just `text`; add a prefix or suffix.")
        return
    if err == "empty":
        await ctx.send("Proxy format cannot be empty.")
        return

    member = members_dict[member_id]
    if not remove_member_proxy_format(member, prefix, suffix):
        await ctx.send("That proxy format was not found on this member.")
        return

    save_systems()
    await ctx.send(
        f"Removed proxy format for **{member.get('name', member_id)}** in {get_scope_label(subsystem_id)}.\nCurrent formats: {render_member_proxy_format(member)}"
    )


@bot.command(name="movemember", aliases=["mvm"])
async def movemember_prefix(
    ctx: commands.Context,
    member_id: str,
    to_scope: str,
    from_scope: str = None,
):
    user_id = ctx.author.id
    system_id = get_user_system_id(user_id)
    if not system_id:
        await ctx.send("You must register a main system first using /register.")
        return

    to_clean = (to_scope or "").strip().lower()
    if to_clean in {"main", "none", "-"}:
        to_subsystem_id = None
    else:
        to_subsystem_id = to_scope

    from_subsystem_id = None
    if from_scope is not None:
        from_clean = from_scope.strip().lower()
        from_subsystem_id = None if from_clean in {"main", "none", "-"} else from_scope

    ok, message, old_scope, new_scope = move_member_between_scopes(
        system_id,
        member_id,
        to_subsystem_id=to_subsystem_id,
        from_subsystem_id=from_subsystem_id,
    )
    if not ok:
        await ctx.send(message)
        return

    save_systems()
    await ctx.send(f"Moved member `{member_id}` from {get_scope_label(old_scope)} to {get_scope_label(new_scope)}.")


@bot.command(name="membersort", aliases=["ms"])
async def membersort_prefix(ctx: commands.Context, mode: str = None):
    if mode is None:
        await ctx.send("Usage: Cor;membersort <id|alphabetical>")
        return

    raw_mode = mode.strip().lower()
    if raw_mode in {"id", "ids", "number", "numeric"}:
        normalized = "id"
    elif raw_mode in {"alphabetical", "alpha", "name", "names", "a-z", "az"}:
        normalized = "alphabetical"
    else:
        await ctx.send("Invalid mode. Use `id` or `alphabetical`.")
        return

    user_id = ctx.author.id
    system_id = get_user_system_id(user_id)
    if not system_id:
        await ctx.send("You must register a main system first using /register.")
        return

    system = systems_data.get("systems", {}).get(system_id)
    if not system:
        await ctx.send("System not found.")
        return

    settings = system.setdefault("member_display", {})
    settings["sort_mode"] = normalized
    save_systems()
    label = "Alphabetical" if normalized == "alphabetical" else "ID"
    await ctx.send(f"Member list sorting set to **{label}**.")


@bot.command(name="creategroup", aliases=["cg"])
async def creategroup_prefix(ctx: commands.Context, name: str, parent_group_id: str = None):
    user_id = ctx.author.id
    system_id = get_user_system_id(user_id)
    if not system_id:
        await ctx.send("You must register a main system first using /register.")
        return

    system = systems_data.get("systems", {}).get(system_id)
    if not system:
        await ctx.send("System not found.")
        return

    settings = get_group_settings(system)
    groups = settings.get("groups", {})

    if parent_group_id is not None:
        parent_group_id = str(parent_group_id)
        if parent_group_id not in groups:
            await ctx.send("Parent group not found.")
            return

    group_id = get_next_group_id(system)
    groups[group_id] = {
        "id": group_id,
        "name": name.strip() or f"Group {group_id}",
        "parent_id": parent_group_id,
    }
    settings.setdefault("order", []).append(group_id)
    save_systems()

    parent_msg = f" under `{parent_group_id}`" if parent_group_id else ""
    await ctx.send(f"Created group **{groups[group_id]['name']}** (`{group_id}`){parent_msg}.")


@bot.command(name="editgroup", aliases=["eg"])
async def editgroup_prefix(ctx: commands.Context, group_id: str, field: str, *, value: str = None):
    user_id = ctx.author.id
    system_id = get_user_system_id(user_id)
    if not system_id:
        await ctx.send("You must register a main system first using /register.")
        return

    system = systems_data.get("systems", {}).get(system_id)
    if not system:
        await ctx.send("System not found.")
        return

    settings = get_group_settings(system)
    groups = settings.get("groups", {})
    group_id = str(group_id)
    group = groups.get(group_id)
    if not group:
        await ctx.send("Group not found.")
        return

    field_name = (field or "").strip().lower()
    if field_name == "name":
        if not value or not value.strip():
            await ctx.send("Usage: Cor;editgroup <group_id> name <new_name>")
            return
        group["name"] = value.strip()
    elif field_name in {"parent", "parent_id"}:
        cleaned_value = (value or "").strip()
        if cleaned_value.lower() in {"", "none", "clear", "-"}:
            group["parent_id"] = None
        else:
            parent_group_id = str(cleaned_value)
            if parent_group_id not in groups:
                await ctx.send("Parent group not found.")
                return
            if parent_group_id == group_id:
                await ctx.send("A group cannot be its own parent.")
                return
            check_id = parent_group_id
            seen = set()
            while check_id and check_id not in seen:
                seen.add(check_id)
                if check_id == group_id:
                    await ctx.send("Invalid parent: this would create a cycle.")
                    return
                next_parent = groups.get(check_id, {}).get("parent_id")
                check_id = str(next_parent) if next_parent is not None else None
            group["parent_id"] = parent_group_id
    else:
        await ctx.send("Unknown field. Use `name` or `parent`.")
        return

    save_systems()
    await ctx.send(f"Updated group **{group['name']}** (`{group_id}`).")


@bot.command(name="deletegroup", aliases=["dg"])
async def deletegroup_prefix(ctx: commands.Context, group_id: str, delete_children: str = "true"):
    parsed_delete_children = parse_bool_token(delete_children)
    if parsed_delete_children is None:
        await ctx.send("Invalid delete_children value. Use true/false.")
        return

    user_id = ctx.author.id
    system_id = get_user_system_id(user_id)
    if not system_id:
        await ctx.send("You must register a main system first using /register.")
        return

    system = systems_data.get("systems", {}).get(system_id)
    if not system:
        await ctx.send("System not found.")
        return

    settings = get_group_settings(system)
    groups = settings.get("groups", {})
    group_id = str(group_id)
    if group_id not in groups:
        await ctx.send("Group not found.")
        return

    to_delete = {group_id}
    if parsed_delete_children:
        changed = True
        while changed:
            changed = False
            for gid, group in groups.items():
                parent_id = group.get("parent_id")
                if parent_id is not None and str(parent_id) in to_delete and gid not in to_delete:
                    to_delete.add(gid)
                    changed = True

    for _, members_dict in iter_system_member_dicts(system):
        for member in members_dict.values():
            existing = member.get("groups", []) or []
            member["groups"] = [str(gid) for gid in existing if str(gid) not in to_delete]

    for gid in list(to_delete):
        groups.pop(gid, None)
    settings["order"] = [gid for gid in settings.get("order", []) if gid not in to_delete]
    save_systems()
    await ctx.send(f"Deleted {len(to_delete)} group(s): {', '.join(sorted(to_delete))}")


@bot.command(name="listgroups", aliases=["lg"])
async def listgroups_prefix(ctx: commands.Context):
    user_id = ctx.author.id
    system_id = get_user_system_id(user_id)
    if not system_id:
        await ctx.send("You must register a main system first using /register.")
        return

    system = systems_data.get("systems", {}).get(system_id)
    if not system:
        await ctx.send("System not found.")
        return

    settings = get_group_settings(system)
    groups = settings.get("groups", {})
    order = settings.get("order", [])
    if not groups:
        await ctx.send("No groups found. Use Cor;creategroup to add one.")
        return

    lines = []
    for gid in order:
        if gid not in groups:
            continue
        path = get_group_path_text(groups, gid)
        lines.append(f"- `{gid}`: {path}")

    embed = discord.Embed(
        title="Group List",
        description="\n".join(lines) if lines else "No groups found.",
        color=discord.Color.dark_teal(),
    )
    await ctx.send(embed=embed)


@bot.command(name="grouporder", aliases=["go"])
async def grouporder_prefix(ctx: commands.Context, *, group_ids: str):
    user_id = ctx.author.id
    system_id = get_user_system_id(user_id)
    if not system_id:
        await ctx.send("You must register a main system first using /register.")
        return

    system = systems_data.get("systems", {}).get(system_id)
    if not system:
        await ctx.send("System not found.")
        return

    settings = get_group_settings(system)
    groups = settings.get("groups", {})
    tokens = [t.strip() for t in group_ids.replace(";", ",").split(",") if t.strip()]
    if not tokens:
        await ctx.send("Provide at least one group ID.")
        return

    unknown = [gid for gid in tokens if gid not in groups]
    if unknown:
        await ctx.send(f"Unknown group IDs: {', '.join(unknown)}")
        return

    new_order = []
    seen = set()
    for gid in tokens:
        if gid not in seen:
            new_order.append(gid)
            seen.add(gid)
    for gid in settings.get("order", []):
        if gid in groups and gid not in seen:
            new_order.append(gid)
            seen.add(gid)

    settings["order"] = new_order
    save_systems()
    await ctx.send(f"Group order updated: {', '.join(new_order)}")


@bot.command(name="grouporderui", aliases=["goui"])
async def grouporderui_prefix(ctx: commands.Context):
    user_id = ctx.author.id
    system_id = get_user_system_id(user_id)
    if not system_id:
        await ctx.send("You must register a main system first using /register.")
        return

    system = systems_data.get("systems", {}).get(system_id)
    if not system:
        await ctx.send("System not found.")
        return

    settings = get_group_settings(system)
    if not settings.get("groups"):
        await ctx.send("No groups found. Use Cor;creategroup to add one.")
        return

    view = GroupOrderView(ctx.author.id, system)
    await ctx.send(embed=view.current_embed(), view=view)


@bot.command(name="addmembergroup", aliases=["amg"])
async def addmembergroup_prefix(ctx: commands.Context, member_id: str, group_id: str, subsystem_id: str = None):
    user_id = ctx.author.id
    system_id = get_user_system_id(user_id)
    if not system_id:
        await ctx.send("You must register using /register.")
        return

    system = systems_data.get("systems", {}).get(system_id)
    members_dict = get_system_members(system_id, subsystem_id)
    if members_dict is None:
        await ctx.send("Subsystem not found.")
        return
    if member_id not in members_dict:
        await ctx.send(f"Member not found in {get_scope_label(subsystem_id)}.")
        return

    groups = get_group_settings(system).get("groups", {})
    group_id = str(group_id)
    if group_id not in groups:
        await ctx.send("Group not found.")
        return

    member = members_dict[member_id]
    assigned = [str(gid) for gid in (member.get("groups", []) or [])]
    if group_id not in assigned:
        assigned.append(group_id)
        member["groups"] = assigned
        save_systems()

    await ctx.send(f"Added group `{group_id}` to **{member['name']}**.")


@bot.command(name="removemembergroup", aliases=["rmg"])
async def removemembergroup_prefix(ctx: commands.Context, member_id: str, group_id: str, subsystem_id: str = None):
    user_id = ctx.author.id
    system_id = get_user_system_id(user_id)
    if not system_id:
        await ctx.send("You must register using /register.")
        return

    members_dict = get_system_members(system_id, subsystem_id)
    if members_dict is None:
        await ctx.send("Subsystem not found.")
        return
    if member_id not in members_dict:
        await ctx.send(f"Member not found in {get_scope_label(subsystem_id)}.")
        return

    member = members_dict[member_id]
    group_id = str(group_id)
    assigned = [str(gid) for gid in (member.get("groups", []) or [])]
    if group_id not in assigned:
        await ctx.send(f"Member is not in group `{group_id}`.")
        return

    member["groups"] = [gid for gid in assigned if gid != group_id]
    save_systems()
    await ctx.send(f"Removed group `{group_id}` from **{member['name']}**.")


@bot.command(name="membergroups", aliases=["mg"])
async def membergroups_prefix(ctx: commands.Context, member_id: str, subsystem_id: str = None):
    user_id = ctx.author.id
    system_id = get_user_system_id(user_id)
    if not system_id:
        await ctx.send("You must register using /register.")
        return

    system = systems_data.get("systems", {}).get(system_id)
    members_dict = get_system_members(system_id, subsystem_id)
    if members_dict is None:
        await ctx.send("Subsystem not found.")
        return
    if member_id not in members_dict:
        await ctx.send(f"Member not found in {get_scope_label(subsystem_id)}.")
        return

    member = members_dict[member_id]
    embed = discord.Embed(
        title=f"Groups - {member.get('name', member_id)}",
        description=format_member_group_lines(system, member),
        color=discord.Color.teal(),
    )
    await ctx.send(embed=embed)


@bot.command(name="register", aliases=["reg"])
async def register_prefix(ctx: commands.Context, *, system_name: str = None):
    if not system_name:
        await ctx.send("Usage: Cor;register <system_name>")
        return

    user_id = ctx.author.id
    if get_user_system_id(user_id):
        await ctx.send("You already have a registered profile.")
        return

    next_id = str(max([int(sid) for sid in systems_data["systems"].keys()] or [0]) + 1)
    systems_data["systems"][next_id] = {
        "system_name": system_name,
        "mode": "system",
        "owner_id": str(user_id),
        "system_privacy": "private",
        "members": {},
        "subsystems": {},
        "timezone": "UTC",
        "system_tag": None,
        "system_profile": {
            "collective_pronouns": None,
            "description": None,
            "profile_pic": None,
            "banner": None,
            "color": "00DE9B"
        },
        "checkins": {
            "entries": [],
            "weekly_dm_enabled": True,
            "last_weekly_summary_week": None
        },
        "front_reminders": {
            "enabled": False,
            "hours": 4
        },
        "external_messages": {
            "accept": False,
            "blocked_users": [],
            "muted_users": [],
            "trusted_only": False,
            "trusted_users": [],
            "temp_blocks": {},
            "pending_requests": [],
            "audit_log": [],
            "message_max_length": 1500,
            "inbox_retention_days": 30,
            "target_rate_seconds": EXTERNAL_TARGET_LIMIT_SECONDS,
            "quiet_hours": {"enabled": False, "start": 23, "end": 7},
            "delivery_mode": "private_summary"
        },
        "autoproxy": {
            "mode": "off",
            "latch_scope_id": None,
            "latch_member_id": None
        },
        "autoproxy_server_overrides": {},
        "server_appearance_overrides": {},
        "server_member_overrides": {},
        "group_settings": {
            "groups": {},
            "order": []
        }
    }
    save_systems()
    await ctx.send(f"System **{system_name}** registered! You can now add members and subsystems.")


@bot.command(name="createsubsystem", aliases=["css"])
async def createsubsystem_prefix(ctx: commands.Context, *, subsystem_name: str = None):
    if not subsystem_name:
        await ctx.send("Usage: Cor;createsubsystem <subsystem_name>")
        return

    user_id = ctx.author.id
    system_id = get_user_system_id(user_id)
    if not system_id:
        await ctx.send("You must register a main system first using /register.")
        return

    system = systems_data["systems"][system_id]
    subsystems = system.setdefault("subsystems", {})
    next_sub_id = chr(97 + len(subsystems))
    subsystems[next_sub_id] = {
        "subsystem_name": subsystem_name,
        "members": {},
        "description": None,
        "color": "00DE9B"
    }
    save_systems()
    await ctx.send(f"Subsystem **{subsystem_name}** created with ID `{next_sub_id}`.")


@bot.command(name="editsubsystem", aliases=["es"])
async def editsubsystem_prefix(ctx: commands.Context, subsystem_id: str = None, *, new_name: str = None):
    if not subsystem_id or not new_name:
        await ctx.send("Usage: Cor;editsubsystem <subsystem_id> <new_name>")
        return

    user_id = ctx.author.id
    system_id = get_user_system_id(user_id)
    if not system_id:
        await ctx.send("You must register a main system first using /register.")
        return

    system = systems_data["systems"][system_id]
    subsystems = system.get("subsystems", {})

    if subsystem_id not in subsystems:
        await ctx.send(f"Subsystem `{subsystem_id}` not found.")
        return

    old_name = subsystems[subsystem_id].get("subsystem_name", "Unnamed")
    subsystems[subsystem_id]["subsystem_name"] = new_name
    save_systems()

    await ctx.send(f"Subsystem `{subsystem_id}` renamed from **{old_name}** to **{new_name}**.")


@bot.command(name="viewsubsystemcard", aliases=["vssc"])
async def viewsubsystemcard_prefix(ctx: commands.Context, subsystem_id: str = None, target_user_id: str = None):
    if not subsystem_id:
        await ctx.send("Usage: Cor;viewsubsystemcard <subsystem_id> [target_user_id]")
        return

    requester_id = ctx.author.id
    system_id, system, target_owner_id, error = resolve_target_system_for_view(requester_id, target_user_id)
    if error:
        await ctx.send(error)
        return

    if str(target_owner_id) != str(requester_id) and not can_view_system_data(system, requester_id):
        await ctx.send("You do not have permission to view this subsystem card.")
        return

    subsystems = system.get("subsystems", {})
    
    if subsystem_id not in subsystems:
        await ctx.send(f"Subsystem `{subsystem_id}` not found.")
        return
    
    subsystem_data = subsystems[subsystem_id]
    embed = build_subsystem_card_embed(subsystem_data, subsystem_id, system)
    
    await ctx.send(embed=embed)


@bot.command(name="editsubsystemcard", aliases=["esc"])
async def editsubsystemcard_prefix(ctx: commands.Context, subsystem_id: str = None, *, args: str = None):
    if not subsystem_id:
        await ctx.send(
            "Usage:\n"
            "• `Cor;esc <id> description <text>` - Set description\n"
            "• `Cor;esc <id> color #HEX` - Set color\n"
            "• `Cor;esc <id> set_pic` (with image attached) - Set profile picture\n"
            "• `Cor;esc <id> set_banner` (with image attached) - Set banner\n"
            "• `Cor;esc <id> clear_pic` - Clear profile picture\n"
            "• `Cor;esc <id> clear_banner` - Clear banner"
        )
        return

    user_id = ctx.author.id
    system_id = get_user_system_id(user_id)
    if not system_id:
        await ctx.send("You must register a main system first using /register.")
        return

    system = systems_data["systems"][system_id]
    subsystems = system.get("subsystems", {})
    
    if subsystem_id not in subsystems:
        await ctx.send(f"Subsystem `{subsystem_id}` not found.")
        return
    
    subsystem_data = subsystems[subsystem_id]
    
    # Handle attachments first
    if ctx.message.attachments and args:
        parts = args.split(None, 1)
        if len(parts) >= 1:
            arg_type = parts[0].lower()
            
            if arg_type == "set_pic":
                try:
                    subsystem_data["profile_pic"] = ctx.message.attachments[0].url
                    save_systems()
                    embed = build_subsystem_card_embed(subsystem_data, subsystem_id, system)
                    await ctx.send("Profile picture updated.", embed=embed)
                    return
                except Exception as e:
                    await ctx.send(f"Failed to set profile picture: {e}")
                    return
            
            elif arg_type == "set_banner":
                try:
                    subsystem_data["banner"] = ctx.message.attachments[0].url
                    save_systems()
                    embed = build_subsystem_card_embed(subsystem_data, subsystem_id, system)
                    await ctx.send("Banner updated.", embed=embed)
                    return
                except Exception as e:
                    await ctx.send(f"Failed to set banner: {e}")
                    return
    
    # Parse text arguments
    if args:
        parts = args.split(None, 1)
        if len(parts) >= 1:
            arg_type = parts[0].lower()
            arg_value = parts[1] if len(parts) > 1 else None
            
            if arg_type == "description" and arg_value:
                subsystem_data["description"] = arg_value.strip()
            elif arg_type == "color" and arg_value:
                try:
                    subsystem_data["color"] = normalize_hex(arg_value)
                except ValueError as e:
                    await ctx.send(str(e))
                    return
            elif arg_type == "clear_pic":
                subsystem_data.pop("profile_pic", None)
            elif arg_type == "clear_banner":
                subsystem_data.pop("banner", None)
    
    save_systems()
    embed = build_subsystem_card_embed(subsystem_data, subsystem_id, system)
    await ctx.send(embed=embed)



@bot.command(name="listsubsystems", aliases=["lss"])
async def listsubsystems_prefix(ctx: commands.Context, target_user_id: str = None):
    requester_id = ctx.author.id
    system_id, system, target_owner_id, error = resolve_target_system_for_view(requester_id, target_user_id)
    if error:
        await ctx.send(error)
        return

    if str(target_owner_id) != str(requester_id) and not can_view_system_data(system, requester_id):
        await ctx.send("You do not have permission to view this subsystem list.")
        return

    subsystems = system.get("subsystems", {})

    if not subsystems:
        await ctx.send("You have no subsystems yet. Use /createsubsystem to add one.")
        return

    lines = []
    for sub_id, sub_data in sorted(subsystems.items()):
        sub_name = sub_data.get("subsystem_name", "Unnamed Subsystem")
        member_count = len(sub_data.get("members", {}))
        lines.append(f"ID `{sub_id}` - {sub_name} ({member_count} members)")

    embed = discord.Embed(
        title=f"{system.get('system_name', 'System')} Subsystems",
        description="\n".join(lines),
        color=discord.Color.blurple()
    )
    await ctx.send(embed=embed)


@bot.command(name="systemtag", aliases=["stag"])
async def systemtag_prefix(ctx: commands.Context, *, value: str = None):
    user_id = ctx.author.id
    system_id = get_user_system_id(user_id)
    if not system_id:
        await ctx.send("You must register a main system first using /register.")
        return

    system = systems_data["systems"].get(system_id)
    if not system:
        await ctx.send("System not found.")
        return

    if value is None:
        current_tag = get_system_proxy_tag(system)
        if current_tag:
            await ctx.send(f"Current system tag: {current_tag}")
        else:
            await ctx.send("No system tag set. Use Cor;systemtag <value> to set one.")
        return

    cleaned = value.strip()
    if cleaned.lower() in {"clear", "none", "off", "-"}:
        system["system_tag"] = None
        save_systems()
        await ctx.send("System tag cleared.")
        return

    if not cleaned:
        await ctx.send("Tag cannot be blank.")
        return

    system["system_tag"] = cleaned
    save_systems()
    await ctx.send(f"System tag set to: {cleaned}")


@bot.command(name="systemprivacy", aliases=["spv"])
async def systemprivacy_prefix(ctx: commands.Context, level: str = None):
    if level is None:
        await ctx.send("Usage: Cor;systemprivacy <private|trusted|public>")
        return

    user_id = ctx.author.id
    system_id = get_user_system_id(user_id)
    if not system_id:
        await ctx.send("You must register a main system first using /register.")
        return

    system = systems_data["systems"].get(system_id)
    if not system:
        await ctx.send("System not found.")
        return

    raw_level = str(level).strip().lower()
    if raw_level not in PROFILE_PRIVACY_LEVELS:
        await ctx.send("Invalid privacy level. Use: private, trusted, or public.")
        return

    cleaned = raw_level
    system["system_privacy"] = cleaned
    save_systems()
    await ctx.send(f"System privacy set to **{cleaned}**.")


@bot.command(name="alterprivacy", aliases=["apv"])
async def alterprivacy_prefix(ctx: commands.Context, member_id: str = None, level: str = None, subsystem_id: str = None):
    if member_id is None or level is None:
        await ctx.send("Usage: Cor;alterprivacy <member_id> <private|trusted|public> [subsystem_id]")
        return

    user_id = ctx.author.id
    system_id = get_user_system_id(user_id)
    if not system_id:
        await ctx.send("You must register a main system first using /register.")
        return

    members_dict = get_system_members(system_id, subsystem_id)
    if members_dict is None:
        await ctx.send("Subsystem not found.")
        return
    if member_id not in members_dict:
        await ctx.send(f"Member not found in {get_scope_label(subsystem_id)}.")
        return

    raw_level = str(level).strip().lower()
    if raw_level not in PROFILE_PRIVACY_LEVELS:
        await ctx.send("Invalid privacy level. Use: private, trusted, or public.")
        return

    cleaned = raw_level
    member = members_dict[member_id]
    member["privacy_level"] = cleaned
    save_systems()
    await ctx.send(f"Privacy for **{member.get('name', member_id)}** set to **{cleaned}**.")


@bot.command(name="privacystatus", aliases=["pvs"])
async def privacystatus_prefix(ctx: commands.Context):
    user_id = ctx.author.id
    system_id = get_user_system_id(user_id)
    if not system_id:
        await ctx.send("You must register first using /register.")
        return

    system = systems_data["systems"].get(system_id)
    if not system:
        await ctx.send("System not found.")
        return

    counts = {"private": 0, "trusted": 0, "public": 0}
    total_members = 0
    sample_lines = []
    for scope_id, members_dict in iter_system_member_dicts(system):
        for member_id, member in members_dict.items():
            level = get_member_privacy_level(member)
            counts[level] = counts.get(level, 0) + 1
            total_members += 1
            if len(sample_lines) < 12:
                sample_lines.append(f"• {member.get('name', member_id)} (`{member_id}`) — {level} ({get_scope_label(scope_id)})")

    lines = [
        "**Privacy Status**",
        f"System privacy: **{get_system_privacy_level(system)}**",
        f"Trusted users: **{len(get_external_settings(system).get('trusted_users', []))}**",
        f"Alter privacy totals -> private: **{counts.get('private', 0)}**, trusted: **{counts.get('trusted', 0)}**, public: **{counts.get('public', 0)}**, total: **{total_members}**",
    ]
    if sample_lines:
        lines.append("\n**Sample Alters**")
        lines.extend(sample_lines)
    await ctx.send("\n".join(lines))

@bot.command(name="serveridentity", aliases=["si"])
async def serveridentity_prefix(ctx: commands.Context, field: str = None, *, value: str = None):
    VALID_FIELDS = {"display_name", "tag", "icon", "clear_display_name", "clear_tag", "clear_icon"}
    if field is None or field.lower() not in VALID_FIELDS:
        await ctx.send(
            "Usage: `Cor;serveridentity <field> [value]`\n"
            "Fields: `display_name <name>`, `tag <tag>`, `icon <url or attach image>`, "
            "`clear_display_name`, `clear_tag`, `clear_icon`"
        )
        return

    if ctx.guild is None:
        await ctx.send("This command can only be used in a server.")
        return

    user_id = ctx.author.id
    system_id = get_user_system_id(user_id)
    if not system_id:
        await ctx.send("You must register a main system first using /register.")
        return

    system = systems_data.get("systems", {}).get(system_id)
    if not system:
        await ctx.send("System not found.")
        return

    guild_key = str(ctx.guild.id)
    overrides = system.setdefault("server_appearance_overrides", {})
    server_app = overrides.setdefault(guild_key, {})
    f = field.lower()

    if f == "clear_display_name":
        server_app.pop("display_name", None)
        msg = "Server display name cleared."
    elif f == "clear_tag":
        server_app.pop("system_tag", None)
        msg = "Server system tag cleared."
    elif f == "clear_icon":
        server_app.pop("profile_pic", None)
        msg = "Server icon cleared."
    elif f == "display_name":
        if not value:
            await ctx.send("Usage: `Cor;serveridentity display_name <name>`")
            return
        server_app["display_name"] = value.strip() or None
        msg = f"Server display name set to **{value.strip()}**."
    elif f == "tag":
        if not value:
            await ctx.send("Usage: `Cor;serveridentity tag <value>`")
            return
        server_app["system_tag"] = value.strip() or None
        msg = f"Server system tag set to **{value.strip()}**."
    elif f == "icon":
        attachment = ctx.message.attachments[0] if ctx.message.attachments else None
        if attachment:
            server_app["profile_pic"] = attachment.url
            msg = "Server icon updated from attachment."
        elif value and value.strip().startswith("http"):
            server_app["profile_pic"] = value.strip()
            msg = "Server icon updated."
        else:
            await ctx.send("Provide an image attachment or a URL: `Cor;serveridentity icon <url>`")
            return
    else:
        msg = "Unknown field."

    # Remove override entry if now empty
    if not server_app:
        overrides.pop(guild_key, None)

    save_systems()
    await ctx.send(msg)


@bot.command(name="serveridentitystatus", aliases=["sis"])
async def serveridentitystatus_prefix(ctx: commands.Context):
    user_id = ctx.author.id
    system_id = get_user_system_id(user_id)
    if not system_id:
        await ctx.send("You must register a main system first using /register.")
        return

    system = systems_data.get("systems", {}).get(system_id)
    if not system:
        await ctx.send("System not found.")
        return

    guild_id = ctx.guild.id if ctx.guild else None
    server_app = get_server_appearance(system, guild_id)
    global_tag = system.get("system_tag")
    global_name = system.get("system_name", "Unnamed System")
    global_profile = get_system_profile(system)
    global_icon = global_profile.get("profile_pic")

    eff_name = (server_app.get("display_name") if server_app else None) or global_name
    eff_tag = (server_app.get("system_tag") if server_app else None) or global_tag
    eff_icon = (server_app.get("profile_pic") if server_app else None) or global_icon

    lines = [
        "**Display name:**",
        f"  Global: {global_name}",
        f"  Server: {(server_app.get('display_name') if server_app else None) or 'none'}",
        f"  Effective: **{eff_name}**",
        "",
        "**System tag:**",
        f"  Global: {global_tag or 'not set'}",
        f"  Server: {(server_app.get('system_tag') if server_app else None) or 'none'}",
        f"  Effective: **{eff_tag or 'not set'}**",
        "",
        "**Icon:**",
        f"  Global: {'set' if global_icon else 'not set'}",
        f"  Server: {'set' if (server_app and server_app.get('profile_pic')) else 'none'}",
        f"  Effective: **{'set' if eff_icon else 'not set'}**",
    ]
    await ctx.send("\n".join(lines))


@bot.command(name="servermemberidentity", aliases=["smi"])
async def servermemberidentity_prefix(
    ctx: commands.Context,
    member_id: str = None,
    field: str = None,
    subsystem_id: str = None,
    *,
    value: str = None,
):
    if member_id is None or field is None:
        await ctx.send(
            "Usage: `Cor;servermemberidentity <member_id> <field> [subsystem_id] [value]`\n"
            "Fields: `display_name`, `tag`, `icon`, `clear_display_name`, `clear_tag`, `clear_icon`"
        )
        return

    if ctx.guild is None:
        await ctx.send("This command can only be used in a server.")
        return

    VALID_FIELDS = {"display_name", "tag", "icon", "clear_display_name", "clear_tag", "clear_icon"}
    f = field.lower()
    if f not in VALID_FIELDS:
        await ctx.send("Invalid field. Use `display_name`, `tag`, `icon`, `clear_display_name`, `clear_tag`, or `clear_icon`.")
        return

    user_id = ctx.author.id
    system_id = get_user_system_id(user_id)
    if not system_id:
        await ctx.send("You must register a main system first using /register.")
        return

    system = systems_data.get("systems", {}).get(system_id)
    if not system:
        await ctx.send("System not found.")
        return

    resolved_scope_id, member, error_key = resolve_member_for_override(system, member_id, subsystem_id=subsystem_id)
    if error_key == "subsystem_not_found":
        await ctx.send("Subsystem not found.")
        return
    if error_key == "ambiguous":
        await ctx.send("Member ID exists in multiple scopes. Provide `subsystem_id` to target the correct member.")
        return
    if error_key == "not_found" or not member:
        await ctx.send(f"Member not found in {get_scope_label(subsystem_id)}.")
        return

    guild_key = str(ctx.guild.id)
    by_guild = system.setdefault("server_member_overrides", {}).setdefault(guild_key, {})
    member_key = str(member.get("id") or member_id)
    override = by_guild.setdefault(member_key, {})

    if f == "clear_display_name":
        override.pop("display_name", None)
        msg = "Member server display name cleared."
    elif f == "clear_tag":
        override.pop("system_tag", None)
        msg = "Member server tag suffix cleared."
    elif f == "clear_icon":
        override.pop("profile_pic", None)
        msg = "Member server icon cleared."
    elif f == "display_name":
        if not value:
            await ctx.send("Usage: `Cor;servermemberidentity <member_id> display_name [subsystem_id] <name>`")
            return
        override["display_name"] = value.strip() or None
        msg = f"Member server display name set to **{value.strip()}**."
    elif f == "tag":
        if not value:
            await ctx.send("Usage: `Cor;servermemberidentity <member_id> tag [subsystem_id] <value>`")
            return
        override["system_tag"] = value.strip() or None
        msg = f"Member server tag suffix set to **{value.strip()}**."
    elif f == "icon":
        attachment = ctx.message.attachments[0] if ctx.message.attachments else None
        if attachment:
            override["profile_pic"] = attachment.url
            msg = "Member server icon updated from attachment."
        elif value and value.strip().startswith("http"):
            override["profile_pic"] = value.strip()
            msg = "Member server icon updated."
        else:
            await ctx.send("Provide an image attachment or URL: `Cor;servermemberidentity <member_id> icon [subsystem_id] <url>`")
            return
    else:
        msg = "Unknown field."

    if not override:
        by_guild.pop(member_key, None)
    if not by_guild:
        system.setdefault("server_member_overrides", {}).pop(guild_key, None)

    save_systems()
    await ctx.send(msg)


@bot.command(name="servermemberidentitystatus", aliases=["smis"])
async def servermemberidentitystatus_prefix(ctx: commands.Context, member_id: str = None, subsystem_id: str = None):
    if member_id is None:
        await ctx.send("Usage: `Cor;servermemberidentitystatus <member_id> [subsystem_id]`")
        return

    user_id = ctx.author.id
    system_id = get_user_system_id(user_id)
    if not system_id:
        await ctx.send("You must register a main system first using /register.")
        return

    system = systems_data.get("systems", {}).get(system_id)
    if not system:
        await ctx.send("System not found.")
        return

    resolved_scope_id, member, error_key = resolve_member_for_override(system, member_id, subsystem_id=subsystem_id)
    if error_key == "subsystem_not_found":
        await ctx.send("Subsystem not found.")
        return
    if error_key == "ambiguous":
        await ctx.send("Member ID exists in multiple scopes. Provide `subsystem_id` to target the correct member.")
        return
    if error_key == "not_found" or not member:
        await ctx.send(f"Member not found in {get_scope_label(subsystem_id)}.")
        return

    guild_id = ctx.guild.id if ctx.guild else None
    member_key = str(member.get("id") or member_id)
    member_override = get_server_member_appearance(system, guild_id, member_key) if guild_id else None
    system_override = get_server_appearance(system, guild_id) if guild_id else None
    system_profile = get_system_profile(system)

    base_display = member.get("display_name") or member.get("name", "Unknown")
    eff_display = (member_override.get("display_name") if member_override else None) or base_display

    base_tag = get_system_proxy_tag(system)
    system_level_tag = (system_override.get("system_tag") if system_override else None) or base_tag
    eff_tag = (member_override.get("system_tag") if member_override else None) or system_level_tag

    base_icon = member.get("profile_pic") or system_profile.get("profile_pic")
    system_level_icon = (system_override.get("profile_pic") if system_override else None) or base_icon
    eff_icon = (member_override.get("profile_pic") if member_override else None) or system_level_icon

    lines = [
        f"**Member:** {member.get('name', 'Unknown')} (`{member_key}`) in {get_scope_label(resolved_scope_id)}",
        "",
        "**Display name:**",
        f"  Member default: {base_display}",
        f"  Server member override: {(member_override.get('display_name') if member_override else None) or 'none'}",
        f"  Effective: **{eff_display}**",
        "",
        "**Tag suffix:**",
        f"  Global/system default: {base_tag or 'not set'}",
        f"  Server system override: {(system_override.get('system_tag') if system_override else None) or 'none'}",
        f"  Server member override: {(member_override.get('system_tag') if member_override else None) or 'none'}",
        f"  Effective: **{eff_tag or 'not set'}**",
        "",
        "**Icon:**",
        f"  Member/system default: {'set' if base_icon else 'not set'}",
        f"  Server system override: {'set' if (system_override and system_override.get('profile_pic')) else 'none'}",
        f"  Server member override: {'set' if (member_override and member_override.get('profile_pic')) else 'none'}",
        f"  Effective: **{'set' if eff_icon else 'not set'}**",
    ]
    await ctx.send("\n".join(lines))


@bot.command(name="messageto", aliases=["msg"])
async def messageto_prefix(ctx: commands.Context, member_id: str, *, message: str):
    user_id = ctx.author.id
    system_id = get_user_system_id(user_id)
    if not system_id:
        await ctx.send("You must register using /register.")
        return

    members_dict = get_system_members(system_id, None)
    if members_dict is None:
        await ctx.send("Subsystem not found.")
        return

    if member_id not in members_dict:
        await ctx.send("Member not found in main system.")
        return

    sender_names = [m["name"] for m in members_dict.values() if m.get("current_front")]
    sender = ", ".join(sender_names) if sender_names else "Unknown"

    members_dict[member_id].setdefault("inbox", [])
    members_dict[member_id]["inbox"].append({
        "from": sender,
        "text": message,
        "sent_at": datetime.now(timezone.utc).isoformat()
    })
    save_systems()
    await ctx.send(f"Message sent to **{members_dict[member_id]['name']}**.")


@bot.command(name="sendexternal", aliases=["sxe"])
async def sendexternal_prefix(ctx: commands.Context, target_user_id: str, target_member_id: str, *, message: str):
    sender_user_id = ctx.author.id
    sender_system_id = get_user_system_id(sender_user_id)
    if not sender_system_id:
        await ctx.send("You must register using /register.")
        return

    sender_sanctions = get_user_sanctions(sender_user_id)
    if sender_sanctions.get("external_banned"):
        await ctx.send("You are restricted from external messaging.")
        return
    if is_user_suspended(sender_user_id, scope="external"):
        await ctx.send("You are temporarily suspended from external messaging.")
        return

    sender_system = systems_data["systems"].get(sender_system_id)
    sender_ext_settings = get_external_settings(sender_system) if sender_system else None
    if not sender_ext_settings or not sender_ext_settings.get("accept"):
        await ctx.send("You must enable external messaging first. Use `Cor;allowexternal true`.")
        return

    parsed_target_user_id = parse_discord_user_id(target_user_id)
    if not parsed_target_user_id:
        await ctx.send("Invalid target user ID. Use a numeric Discord ID or mention.")
        return

    if str(sender_user_id) == parsed_target_user_id:
        await ctx.send("Use Cor;messageto for your own system.")
        return

    if not check_and_update_external_rate_limit(sender_user_id):
        await ctx.send(
            f"Rate limited. You can send up to {EXTERNAL_MSG_LIMIT_COUNT} external messages per {EXTERNAL_MSG_LIMIT_SECONDS} seconds."
        )
        return

    target_system_id = get_user_system_id(parsed_target_user_id)
    if not target_system_id:
        await ctx.send("Target user does not have a registered system.")
        return

    target_system = systems_data["systems"].get(target_system_id)
    if not target_system or not sender_system:
        await ctx.send("System data not found.")
        return

    target_settings = get_external_settings(target_system)
    prune_external_temp_blocks(target_settings)
    cleaned = cleanup_external_inbox_entries(target_system)
    if cleaned:
        save_systems()

    if not target_settings.get("accept"):
        await ctx.send("That system is not accepting external messages.")
        return

    if str(sender_user_id) in target_settings.get("blocked_users", []):
        add_external_audit_entry(target_system, "blocked_reject", sender_user_id, details="blocked_users")
        await ctx.send("You are blocked by that system.")
        return

    if str(sender_user_id) in target_settings.get("muted_users", []):
        add_external_audit_entry(target_system, "muted_drop", sender_user_id)
        await ctx.send("Message not delivered.")
        save_systems()
        return

    temp_blocks = target_settings.get("temp_blocks", {})
    until_iso = temp_blocks.get(str(sender_user_id))
    if until_iso:
        add_external_audit_entry(target_system, "tempblock_reject", sender_user_id, details=f"until={until_iso}")
        await ctx.send("You are temporarily blocked by that system.")
        save_systems()
        return

    max_len = int(target_settings.get("message_max_length", 1500))
    if len(message) > max_len:
        await ctx.send(f"Message too long for that recipient. Max length is {max_len} characters.")
        return

    target_rate_seconds = int(target_settings.get("target_rate_seconds", EXTERNAL_TARGET_LIMIT_SECONDS))
    if not check_and_update_external_target_rate_limit(sender_user_id, parsed_target_user_id, target_rate_seconds):
        await ctx.send(f"Slow down. You can send to that recipient once every {target_rate_seconds} seconds.")
        return

    target_members, _, target_member = resolve_target_member_scope(
        target_system,
        target_system_id,
        target_member_id,
        None,
    )
    if target_members is None or target_member is None:
        await ctx.send("Target member not found in that system (main or subsystems).")
        return

    sender_mode = sender_system.get("mode", "system")
    if sender_mode == "singlet":
        sender_name = sender_system.get("system_name") or ctx.author.display_name
    else:
        _, sender_member = get_fronting_member_for_user(sender_user_id)
        sender_name = sender_member.get("name", "Unknown") if sender_member else "Unknown"
    sender_system_name = sender_system.get("system_name", f"System {sender_system_id}")

    if target_settings.get("trusted_only") and str(sender_user_id) not in target_settings.get("trusted_users", []):
        target_settings.setdefault("pending_requests", []).append({
            "sender_user_id": str(sender_user_id),
            "from_system_id": str(sender_system_id),
            "from": f"{sender_name} ({sender_system_name})",
            "target_member_id": str(target_member_id),
            "target_subsystem_id": None,
            "text": message,
            "sent_at": datetime.now(timezone.utc).isoformat(),
        })
        add_external_audit_entry(target_system, "pending_approval", sender_user_id, target_member_id=target_member_id)
        save_systems()
        await ctx.send("Your message is queued pending recipient approval (trusted-senders-only mode).")
        return

    target_members[target_member_id].setdefault("inbox", [])
    target_members[target_member_id]["inbox"].append({
        "from": f"{sender_name} ({sender_system_name})",
        "from_user_id": str(sender_user_id),
        "from_system_id": str(sender_system_id),
        "text": message,
        "sent_at": datetime.now(timezone.utc).isoformat(),
        "external": True,
    })
    add_external_audit_entry(target_system, "delivered", sender_user_id, target_member_id=target_member_id)
    save_systems()

    await ctx.send(f"External message sent to **{target_member['name']}**.")


@bot.command(name="externalprivacy", aliases=["epr"])
async def externalprivacy_prefix(ctx: commands.Context, mode: str):
    cleaned = (mode or "").strip().lower()
    if cleaned not in {"public", "private_summary", "dm_only"}:
        await ctx.send("Invalid mode. Use: public, private_summary, or dm_only.")
        return

    user_id = ctx.author.id
    system_id = get_user_system_id(user_id)
    if not system_id:
        await ctx.send("You must register using /register.")
        return

    system = systems_data["systems"].get(system_id)
    if not system:
        await ctx.send("System not found.")
        return

    settings = get_external_settings(system)
    settings["delivery_mode"] = cleaned
    save_systems()
    await ctx.send(f"External delivery mode set to **{cleaned}**.")


@bot.command(name="externalstatus", aliases=["est"])
async def externalstatus_prefix(ctx: commands.Context):
    user_id = ctx.author.id
    system_id = get_user_system_id(user_id)
    if not system_id:
        await ctx.send("You must register using /register.")
        return

    system = systems_data["systems"].get(system_id)
    if not system:
        await ctx.send("System not found.")
        return

    settings = get_external_settings(system)
    prune_external_temp_blocks(settings)
    save_systems()
    enabled = "enabled" if settings.get("accept") else "disabled"
    mode = settings.get("delivery_mode", "public")
    blocked_count = len(settings.get("blocked_users", []))
    muted_count = len(settings.get("muted_users", []))
    trusted_only = "enabled" if settings.get("trusted_only") else "disabled"
    trusted_count = len(settings.get("trusted_users", []))
    pending_count = len(settings.get("pending_requests", []))
    temp_count = len(settings.get("temp_blocks", {}))
    quiet = settings.get("quiet_hours", {})
    quiet_label = f"{quiet.get('start', 23)}-{quiet.get('end', 7)}" if quiet.get("enabled") else "off"
    await ctx.send(
        f"External messages: **{enabled}**\n"
        f"Delivery mode: **{mode}**\n"
        f"Trusted-only: **{trusted_only}** (trusted: {trusted_count}, pending: {pending_count})\n"
        f"Blocked users: **{blocked_count}** | Muted users: **{muted_count}** | Temp blocks: **{temp_count}**\n"
        f"Quiet hours: **{quiet_label}**\n"
        f"Limits: max chars **{settings.get('message_max_length', 1500)}**, target cooldown **{settings.get('target_rate_seconds', EXTERNAL_TARGET_LIMIT_SECONDS)}s**, retention **{settings.get('inbox_retention_days', 30)}d**"
    )


@bot.command(name="externallimits", aliases=["elim"])
async def externallimits_prefix(ctx: commands.Context, max_chars: int = 1500, target_cooldown_seconds: int = EXTERNAL_TARGET_LIMIT_SECONDS):
    user_id = ctx.author.id
    system_id = get_user_system_id(user_id)
    if not system_id:
        await ctx.send("You must register using /register.")
        return
    if max_chars < 50 or max_chars > 4000:
        await ctx.send("max_chars must be between 50 and 4000.")
        return
    if target_cooldown_seconds < 0 or target_cooldown_seconds > 3600:
        await ctx.send("target_cooldown_seconds must be between 0 and 3600.")
        return
    system = systems_data["systems"].get(system_id)
    settings = get_external_settings(system)
    settings["message_max_length"] = max_chars
    settings["target_rate_seconds"] = target_cooldown_seconds
    save_systems()
    await ctx.send(
        f"External limits updated: max_chars={max_chars}, target cooldown={target_cooldown_seconds}s."
    )


@bot.command(name="externaltrustedonly", aliases=["eto"])
async def externaltrustedonly_prefix(ctx: commands.Context, enabled: str):
    parsed = parse_bool_token(enabled)
    if parsed is None:
        await ctx.send("Invalid value. Use true/false.")
        return

    user_id = ctx.author.id
    system_id = get_user_system_id(user_id)
    if not system_id:
        await ctx.send("You must register using /register.")
        return
    system = systems_data["systems"].get(system_id)
    if not system:
        await ctx.send("System not found.")
        return
    settings = get_external_settings(system)
    settings["trusted_only"] = parsed
    save_systems()
    state = "enabled" if parsed else "disabled"
    await ctx.send(f"Trusted-senders-only mode is now **{state}**.")


@bot.command(name="trustuser", aliases=["tru"])
async def trustuser_prefix(ctx: commands.Context, user_id: str):
    owner_id = ctx.author.id
    system_id = get_user_system_id(owner_id)
    if not system_id:
        await ctx.send("You must register using /register.")
        return
    parsed = parse_discord_user_id(user_id)
    if not parsed:
        await ctx.send("Invalid user ID. Use a numeric Discord ID or mention.")
        return
    system = systems_data["systems"].get(system_id)
    settings = get_external_settings(system)
    trusted = settings.get("trusted_users", [])
    if parsed not in trusted:
        trusted.append(parsed)
        settings["trusted_users"] = trusted
        save_systems()
    await ctx.send(f"Trusted user added: `{parsed}`.")


@bot.command(name="untrustuser", aliases=["utru"])
async def untrustuser_prefix(ctx: commands.Context, user_id: str):
    owner_id = ctx.author.id
    system_id = get_user_system_id(owner_id)
    if not system_id:
        await ctx.send("You must register using /register.")
        return
    parsed = parse_discord_user_id(user_id)
    if not parsed:
        await ctx.send("Invalid user ID. Use a numeric Discord ID or mention.")
        return
    system = systems_data["systems"].get(system_id)
    settings = get_external_settings(system)
    trusted = settings.get("trusted_users", [])
    if parsed in trusted:
        trusted.remove(parsed)
        settings["trusted_users"] = trusted
        save_systems()
    await ctx.send(f"Trusted user removed: `{parsed}`.")


@bot.command(name="trustedusers", aliases=["trus"])
async def trustedusers_prefix(ctx: commands.Context):
    user_id = ctx.author.id
    system_id = get_user_system_id(user_id)
    if not system_id:
        await ctx.send("You must register using /register.")
        return
    system = systems_data["systems"].get(system_id)
    trusted = get_external_settings(system).get("trusted_users", [])
    if not trusted:
        await ctx.send("No trusted users.")
        return
    await ctx.send("Trusted users:\n" + "\n".join([f"- {u}" for u in trusted]))


@bot.command(name="muteuser", aliases=["mu"])
async def muteuser_prefix(ctx: commands.Context, user_id: str):
    owner_id = ctx.author.id
    system_id = get_user_system_id(owner_id)
    if not system_id:
        await ctx.send("You must register using /register.")
        return
    parsed = parse_discord_user_id(user_id)
    if not parsed:
        await ctx.send("Invalid user ID. Use a numeric Discord ID or mention.")
        return
    system = systems_data["systems"].get(system_id)
    settings = get_external_settings(system)
    muted = settings.get("muted_users", [])
    if parsed not in muted:
        muted.append(parsed)
        settings["muted_users"] = muted
        save_systems()
    await ctx.send(f"Muted user ID `{parsed}`.")


@bot.command(name="unmuteuser", aliases=["umu"])
async def unmuteuser_prefix(ctx: commands.Context, user_id: str):
    owner_id = ctx.author.id
    system_id = get_user_system_id(owner_id)
    if not system_id:
        await ctx.send("You must register using /register.")
        return
    parsed = parse_discord_user_id(user_id)
    if not parsed:
        await ctx.send("Invalid user ID. Use a numeric Discord ID or mention.")
        return
    system = systems_data["systems"].get(system_id)
    settings = get_external_settings(system)
    muted = settings.get("muted_users", [])
    if parsed in muted:
        muted.remove(parsed)
        settings["muted_users"] = muted
        save_systems()
    await ctx.send(f"Unmuted user ID `{parsed}`.")


@bot.command(name="mutedusers", aliases=["mus"])
async def mutedusers_prefix(ctx: commands.Context):
    user_id = ctx.author.id
    system_id = get_user_system_id(user_id)
    if not system_id:
        await ctx.send("You must register using /register.")
        return
    system = systems_data["systems"].get(system_id)
    muted = get_external_settings(system).get("muted_users", [])
    if not muted:
        await ctx.send("No muted users.")
        return
    await ctx.send("Muted users:\n" + "\n".join([f"- {u}" for u in muted]))


@bot.command(name="tempblockuser", aliases=["tbu"])
async def tempblockuser_prefix(ctx: commands.Context, user_id: str, hours: int = 24):
    owner_id = ctx.author.id
    system_id = get_user_system_id(owner_id)
    if not system_id:
        await ctx.send("You must register using /register.")
        return
    parsed = parse_discord_user_id(user_id)
    if not parsed:
        await ctx.send("Invalid user ID. Use a numeric Discord ID or mention.")
        return
    if hours < 1 or hours > 720:
        await ctx.send("Hours must be between 1 and 720.")
        return
    system = systems_data["systems"].get(system_id)
    settings = get_external_settings(system)
    until = (datetime.now(timezone.utc) + timedelta(hours=hours)).isoformat()
    settings.setdefault("temp_blocks", {})[parsed] = until
    save_systems()
    await ctx.send(f"Temporarily blocked `{parsed}` for {hours} hour(s).")


@bot.command(name="tempblockedusers", aliases=["tbus"])
async def tempblockedusers_prefix(ctx: commands.Context):
    user_id = ctx.author.id
    system_id = get_user_system_id(user_id)
    if not system_id:
        await ctx.send("You must register using /register.")
        return
    system = systems_data["systems"].get(system_id)
    settings = get_external_settings(system)
    prune_external_temp_blocks(settings)
    blocks = settings.get("temp_blocks", {})
    save_systems()
    if not blocks:
        await ctx.send("No active temporary blocks.")
        return
    lines = [f"- {uid} until {until}" for uid, until in blocks.items()]
    await ctx.send("Temporary blocks:\n" + "\n".join(lines))


@bot.command(name="blockuser", aliases=["bu"])
async def blockuser_prefix(ctx: commands.Context, user_id: str):
    owner_id = ctx.author.id
    system_id = get_user_system_id(owner_id)
    if not system_id:
        await ctx.send("You must register using /register.")
        return

    parsed_user_id = parse_discord_user_id(user_id)
    if not parsed_user_id:
        await ctx.send("Invalid user ID. Use a numeric Discord ID or mention.")
        return

    if str(owner_id) == parsed_user_id:
        await ctx.send("You cannot block yourself.")
        return

    system = systems_data["systems"].get(system_id)
    if not system:
        await ctx.send("System not found.")
        return

    settings = get_external_settings(system)
    blocked = settings.get("blocked_users", [])
    if parsed_user_id in blocked:
        await ctx.send("That user is already blocked.")
        return

    blocked.append(parsed_user_id)
    settings["blocked_users"] = blocked
    save_systems()
    await ctx.send(f"Blocked user ID `{parsed_user_id}`.")


@bot.command(name="unblockuser", aliases=["ubu"])
async def unblockuser_prefix(ctx: commands.Context, user_id: str):
    owner_id = ctx.author.id
    system_id = get_user_system_id(owner_id)
    if not system_id:
        await ctx.send("You must register using /register.")
        return

    parsed_user_id = parse_discord_user_id(user_id)
    if not parsed_user_id:
        await ctx.send("Invalid user ID. Use a numeric Discord ID or mention.")
        return

    system = systems_data["systems"].get(system_id)
    if not system:
        await ctx.send("System not found.")
        return

    settings = get_external_settings(system)
    blocked = settings.get("blocked_users", [])
    if parsed_user_id not in blocked:
        await ctx.send("That user is not blocked.")
        return

    blocked.remove(parsed_user_id)
    settings["blocked_users"] = blocked
    save_systems()
    await ctx.send(f"Unblocked user ID `{parsed_user_id}`.")


@bot.command(name="blockedusers", aliases=["bus"])
async def blockedusers_prefix(ctx: commands.Context):
    user_id = ctx.author.id
    system_id = get_user_system_id(user_id)
    if not system_id:
        await ctx.send("You must register using /register.")
        return

    system = systems_data["systems"].get(system_id)
    if not system:
        await ctx.send("System not found.")
        return

    blocked = get_external_settings(system).get("blocked_users", [])
    if not blocked:
        await ctx.send("No blocked users.")
        return

    await ctx.send("Blocked user IDs:\n" + "\n".join([f"- {uid}" for uid in blocked]))


@bot.command(name="externalpending", aliases=["ep"])
async def externalpending_prefix(ctx: commands.Context):
    user_id = ctx.author.id
    system_id = get_user_system_id(user_id)
    if not system_id:
        await ctx.send("You must register using /register.")
        return
    system = systems_data["systems"].get(system_id)
    pending = get_external_settings(system).get("pending_requests", [])
    if not pending:
        await ctx.send("No pending external requests.")
        return
    counts = {}
    for p in pending:
        sid = str(p.get("sender_user_id"))
        counts[sid] = counts.get(sid, 0) + 1
    lines = [f"- {sid}: {count} message(s)" for sid, count in counts.items()]
    await ctx.send("Pending sender queue:\n" + "\n".join(lines))


@bot.command(name="approveexternal", aliases=["apx"])
async def approveexternal_prefix(ctx: commands.Context, user_id: str, approve: str = "true"):
    owner_id = ctx.author.id
    system_id = get_user_system_id(owner_id)
    if not system_id:
        await ctx.send("You must register using /register.")
        return
    parsed = parse_discord_user_id(user_id)
    if not parsed:
        await ctx.send("Invalid user ID. Use a numeric Discord ID or mention.")
        return
    approve_bool = parse_bool_token(approve)
    if approve_bool is None:
        await ctx.send("Invalid approve value. Use true/false.")
        return
    system = systems_data["systems"].get(system_id)
    if not system:
        await ctx.send("System not found.")
        return

    settings = get_external_settings(system)
    pending = settings.get("pending_requests", [])
    matched = [p for p in pending if str(p.get("sender_user_id")) == parsed]
    remaining = [p for p in pending if str(p.get("sender_user_id")) != parsed]
    settings["pending_requests"] = remaining

    delivered = 0
    if approve_bool and matched:
        trusted = settings.get("trusted_users", [])
        if parsed not in trusted:
            trusted.append(parsed)
            settings["trusted_users"] = trusted
        for p in matched:
            members_dict = get_system_members(system_id, p.get("target_subsystem_id"))
            target_member_id = p.get("target_member_id")
            if members_dict is None or target_member_id not in members_dict:
                continue
            members_dict[target_member_id].setdefault("inbox", []).append({
                "from": p.get("from"),
                "from_user_id": str(p.get("sender_user_id")),
                "from_system_id": str(p.get("from_system_id")),
                "text": p.get("text"),
                "sent_at": p.get("sent_at") or datetime.now(timezone.utc).isoformat(),
                "external": True
            })
            delivered += 1

    add_external_audit_entry(system, "approveexternal" if approve_bool else "rejectexternal", parsed, details=f"processed={len(matched)}, delivered={delivered}")
    save_systems()
    action = "approved" if approve_bool else "rejected"
    await ctx.send(
        f"{action.title()} sender `{parsed}`. Processed {len(matched)} queued message(s), delivered {delivered}."
    )


@bot.command(name="recentexternal", aliases=["rex"])
async def recentexternal_prefix(ctx: commands.Context, limit: int = 10):
    user_id = ctx.author.id
    system_id = get_user_system_id(user_id)
    if not system_id:
        await ctx.send("You must register using /register.")
        return
    if limit < 1 or limit > 25:
        await ctx.send("Limit must be between 1 and 25.")
        return
    system = systems_data["systems"].get(system_id)
    log = get_external_settings(system).get("audit_log", [])[-limit:]
    if not log:
        await ctx.send("No recent external events.")
        return
    lines = []
    for entry in reversed(log):
        ts = entry.get("timestamp", "?")
        action = entry.get("action", "?")
        sender = entry.get("sender_user_id") or "-"
        details = entry.get("details") or ""
        lines.append(f"- {ts} | {action} | sender={sender} {details}")
    await ctx.send("Recent external events:\n" + "\n".join(lines))


@bot.command(name="externalquiethours", aliases=["eqh"])
async def externalquiethours_prefix(ctx: commands.Context, enabled: str, start_hour: int = 23, end_hour: int = 7):
    parsed = parse_bool_token(enabled)
    if parsed is None:
        await ctx.send("Invalid enabled value. Use true/false.")
        return
    user_id = ctx.author.id
    system_id = get_user_system_id(user_id)
    if not system_id:
        await ctx.send("You must register using /register.")
        return
    if start_hour < 0 or start_hour > 23 or end_hour < 0 or end_hour > 23:
        await ctx.send("Hours must be 0-23.")
        return
    system = systems_data["systems"].get(system_id)
    settings = get_external_settings(system)
    settings["quiet_hours"] = {"enabled": parsed, "start": start_hour, "end": end_hour}
    save_systems()
    await ctx.send(
        f"External quiet hours updated: enabled={parsed}, start={start_hour}, end={end_hour}."
    )


@bot.command(name="externalretention", aliases=["ert"])
async def externalretention_prefix(ctx: commands.Context, days: int):
    user_id = ctx.author.id
    system_id = get_user_system_id(user_id)
    if not system_id:
        await ctx.send("You must register using /register.")
        return
    if days < 1 or days > 365:
        await ctx.send("Days must be between 1 and 365.")
        return
    system = systems_data["systems"].get(system_id)
    settings = get_external_settings(system)
    settings["inbox_retention_days"] = days
    save_systems()
    await ctx.send(f"External retention set to {days} day(s).")


@bot.command(name="reportexternal", aliases=["rptx"])
async def reportexternal_prefix(ctx: commands.Context, user_id: str, *, reason: str):
    reporter_id = ctx.author.id
    parsed = parse_discord_user_id(user_id)
    if not parsed:
        await ctx.send("Invalid user ID. Use a numeric Discord ID or mention.")
        return
    if len(reason.strip()) < 5:
        await ctx.send("Please provide a slightly longer reason.")
        return

    state = get_moderation_state()
    reports = state.setdefault("reports", [])
    report_id = f"R{len(reports) + 1:05d}"
    reports.append({
        "id": report_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "reporter_user_id": str(reporter_id),
        "accused_user_id": parsed,
        "reason": reason.strip(),
        "status": "open",
        "resolution": None,
    })
    add_mod_event("report_created", target_user_id=parsed, moderator_user_id=reporter_id, details=report_id)
    save_systems()

    await ctx.send(f"Report `{report_id}` submitted. Moderation has been notified.")


@bot.command(name="modreports", aliases=["mr"])
async def modreports_prefix(ctx: commands.Context, limit: int = 15):
    if limit < 1 or limit > 50:
        await ctx.send("Limit must be between 1 and 50.")
        return
    reports = [r for r in get_moderation_state().get("reports", []) if r.get("status") == "open"]
    if not reports:
        await ctx.send("No open reports.")
        return
    lines = []
    for r in reports[-limit:]:
        lines.append(
            f"- {r.get('id')} | accused={r.get('accused_user_id')} | reporter={r.get('reporter_user_id')} | {r.get('reason')}"
        )
    await ctx.send("Open reports:\n" + "\n".join(lines))


@bot.command(name="modwarn", aliases=["mw"])
async def modwarn_prefix(ctx: commands.Context, user_id: str, *, reason: str = "No reason provided"):
    parsed = parse_discord_user_id(user_id)
    if not parsed:
        await ctx.send("Invalid user ID.")
        return
    sanctions = get_user_sanctions(parsed)
    sanctions["warnings"] = int(sanctions.get("warnings", 0)) + 1
    sanctions["reason"] = reason
    add_mod_event("warn", target_user_id=parsed, moderator_user_id=ctx.author.id, details=reason)
    save_systems()
    await ctx.send(f"Warned `{parsed}`. Total warnings: {sanctions['warnings']}")


@bot.command(name="modsuspend", aliases=["msu"])
async def modsuspend_prefix(ctx: commands.Context, user_id: str, hours: int = 24, scope: str = "external", *, reason: str = "No reason provided"):
    parsed = parse_discord_user_id(user_id)
    if not parsed:
        await ctx.send("Invalid user ID.")
        return
    if hours < 1 or hours > 720:
        await ctx.send("Hours must be between 1 and 720.")
        return
    cleaned_scope = (scope or "external").strip().lower()
    if cleaned_scope not in {"external", "all"}:
        await ctx.send("Invalid scope. Use external or all.")
        return
    sanctions = get_user_sanctions(parsed)
    sanctions["suspended_until"] = (datetime.now(timezone.utc) + timedelta(hours=hours)).isoformat()
    sanctions["scope"] = cleaned_scope
    sanctions["reason"] = reason
    add_mod_event("suspend", target_user_id=parsed, moderator_user_id=ctx.author.id, details=f"scope={cleaned_scope},hours={hours},reason={reason}")
    save_systems()
    await ctx.send(f"Suspended `{parsed}` for {hours} hour(s), scope={cleaned_scope}.")


@bot.command(name="modban", aliases=["mb"])
async def modban_prefix(ctx: commands.Context, user_id: str, scope: str = "external", *, reason: str = "No reason provided"):
    parsed = parse_discord_user_id(user_id)
    if not parsed:
        await ctx.send("Invalid user ID.")
        return
    cleaned_scope = (scope or "external").strip().lower()
    if cleaned_scope not in {"external", "all"}:
        await ctx.send("Invalid scope. Use external or all.")
        return
    sanctions = get_user_sanctions(parsed)
    if cleaned_scope == "all":
        sanctions["bot_banned"] = True
    else:
        sanctions["external_banned"] = True
    sanctions["reason"] = reason
    add_mod_event("ban", target_user_id=parsed, moderator_user_id=ctx.author.id, details=f"scope={cleaned_scope},reason={reason}")
    save_systems()
    await ctx.send(f"Banned `{parsed}` for scope={cleaned_scope}.")


@bot.command(name="modunban", aliases=["mub"])
async def modunban_prefix(ctx: commands.Context, user_id: str, clear_warnings: str = "false"):
    parsed = parse_discord_user_id(user_id)
    if not parsed:
        await ctx.send("Invalid user ID.")
        return
    clear_bool = parse_bool_token(clear_warnings)
    if clear_bool is None:
        await ctx.send("Invalid clear_warnings value. Use true/false.")
        return
    sanctions = get_user_sanctions(parsed)
    sanctions["external_banned"] = False
    sanctions["bot_banned"] = False
    sanctions["suspended_until"] = None
    sanctions["scope"] = "external"
    if clear_bool:
        sanctions["warnings"] = 0
    add_mod_event("unban", target_user_id=parsed, moderator_user_id=ctx.author.id, details=f"clear_warnings={clear_bool}")
    save_systems()
    await ctx.send(f"Cleared bans/suspensions for `{parsed}`.")


@bot.command(name="modappeal", aliases=["map"])
async def modappeal_prefix(ctx: commands.Context, *, message: str):
    if len(message.strip()) < 5:
        await ctx.send("Please provide a longer appeal message.")
        return
    add_mod_event("appeal", target_user_id=ctx.author.id, moderator_user_id=ctx.author.id, details=message.strip())
    save_systems()
    await ctx.send("Your appeal was submitted.")

@bot.command(name="sendmessage", aliases=["schd"])
async def sendmessage_prefix(ctx: commands.Context, *, args: str = None):
    """Schedule a message to be sent to yourself in the future.
    Usage: Cor;sendmessage target:future time:2hrs message:"Your message here"
    """
    user_id = ctx.author.id
    system_id = get_user_system_id(user_id)
    if not system_id:
        await ctx.send("You must register using /register.")
        return
    
    system = systems_data["systems"].get(system_id)
    if not system or system.get("mode") != "singlet":
        await ctx.send("This command is only available for singlet profiles.")
        return
    
    target, time_delta, message, error = parse_sendmessage_args(args)
    if error:
        await ctx.send(f"Error: {error}")
        return
    
    add_scheduled_message(user_id, message, time_delta)
    
    # Format the time nicely
    total_seconds = int(time_delta.total_seconds())
    if total_seconds >= 86400:
        time_display = f"{total_seconds // 86400} day(s)"
    elif total_seconds >= 3600:
        time_display = f"{total_seconds // 3600} hour(s)"
    elif total_seconds >= 60:
        time_display = f"{total_seconds // 60} minute(s)"
    else:
        time_display = f"{total_seconds} second(s)"
    
    send_at = datetime.now(timezone.utc) + time_delta
    await ctx.send(
        f"📅 Message scheduled to be sent to you in **{time_display}** "
        f"(at <t:{int(send_at.timestamp())}:f>).\n\n"
        f"```\n{message}\n```"
    )

@bot.event
async def on_message(message: discord.Message):
    # Never proxy bot/webhook traffic.
    if message.author.bot or message.webhook_id is not None:
        return

    pending_until = PENDING_TIMEZONE_PROMPTS.get(message.author.id)
    if pending_until:
        now_utc = datetime.now(timezone.utc)
        if now_utc > pending_until:
            PENDING_TIMEZONE_PROMPTS.pop(message.author.id, None)
        elif not (message.content.startswith("Cor;") or message.content.startswith("cor;")):
            system_id = get_user_system_id(message.author.id)
            if not system_id:
                PENDING_TIMEZONE_PROMPTS.pop(message.author.id, None)
                await message.channel.send("You must register a main system first using /register.")
                return

            normalized = normalize_timezone_name(message.content)
            if not normalized:
                await message.channel.send("Please provide a valid timezone value (example: EST, UTC, America/New_York).")
                return

            valid = False
            try:
                ZoneInfo(normalized)
                valid = True
            except ZoneInfoNotFoundError:
                if normalized in TIMEZONE_FIXED_OFFSETS or normalized in COMMON_TIMEZONES:
                    valid = True

            if not valid:
                await message.channel.send(
                    "Unknown timezone. Try EST, UTC, America/New_York, Europe/London, or Asia/Tokyo."
                )
                return

            system = systems_data.get("systems", {}).get(system_id)
            if not system:
                PENDING_TIMEZONE_PROMPTS.pop(message.author.id, None)
                await message.channel.send("System not found.")
                return

            system["timezone"] = normalized
            save_systems()
            PENDING_TIMEZONE_PROMPTS.pop(message.author.id, None)
            await message.channel.send(f"Timezone set to **{normalized}**.")
            return

    ctx = await bot.get_context(message)
    if ctx.valid:
        await bot.process_commands(message)
        return

    # In forum posts, never proxy the very first thread message (the post opener).
    is_forum_starter_post = False
    if isinstance(message.channel, discord.Thread):
        parent_channel = message.channel.parent
        if isinstance(parent_channel, discord.ForumChannel):
            is_forum_starter_post = message.id == message.channel.id

    if is_forum_starter_post:
        await bot.process_commands(message)
        return

    # Track proxy resolution for this message.
    explicit_proxy = False
    latch_update = False
    scope_id_for_latch = None
    proxy_member = None
    proxied_text = None
    system = None

    user_id = message.author.id
    system_id = get_user_system_id(user_id)
    if system_id:
        system = systems_data.get("systems", {}).get(system_id)

    if system is None:
        await bot.process_commands(message)
        return

    active_autoproxy_settings, _autoproxy_scope = get_effective_autoproxy_settings(
        system,
        message.guild.id if message.guild else None,
    )

    # Check for global proxy prefix (;;)
    if message.content.startswith(PROXY_PREFIX):
        explicit_proxy = True
        proxied_text = message.content[len(PROXY_PREFIX):].lstrip()
        scope_id_for_latch, proxy_member = get_fronting_member_for_user(message.author.id)
        if not proxy_member:
            await message.channel.send(
                f"{message.author.mention} no currently fronting member was found. Use /switchmember first."
            )
            await bot.process_commands(message)
            return
        latch_update = True
    else:
        # Check explicit member tag proxy first, then autoproxy mode fallback.
        scope_id_for_latch, proxy_member, tagged_text = find_tagged_proxy_member(system, message.content)
        if proxy_member:
            explicit_proxy = True
            latch_update = True
            proxied_text = tagged_text
        else:
            mode = active_autoproxy_settings.get("mode", "off")
            if mode == "front":
                scope_id_for_latch, proxy_member = get_fronting_member_for_user(message.author.id)
                proxied_text = message.content
            elif mode == "latch":
                scope_id_for_latch = active_autoproxy_settings.get("latch_scope_id")
                latch_member_id = active_autoproxy_settings.get("latch_member_id")
                proxy_member = get_member_from_scope(system, scope_id_for_latch, latch_member_id)
                proxied_text = message.content

    # If no proxy triggered, process as normal command
    if not proxy_member:
        await bot.process_commands(message)
        return

    proxied_files = []
    attachment_fallback_urls = []
    for attachment in message.attachments:
        try:
            proxied_files.append(await attachment.to_file())
        except (discord.HTTPException, discord.Forbidden):
            attachment_fallback_urls.append(attachment.url)

    if not proxied_text and not proxied_files and not attachment_fallback_urls:
        await bot.process_commands(message)
        return

    webhook = await get_or_create_proxy_webhook(message.channel)
    if webhook is None:
        await message.channel.send(
            "I can't proxy in this channel type."
        )
        await bot.process_commands(message)
        return

    final_content = proxied_text
    if attachment_fallback_urls:
        attachments_text = "\n".join(attachment_fallback_urls)
        final_content = f"{proxied_text}\n{attachments_text}" if proxied_text else attachments_text

    reply_embed = None
    if message.reference and message.reference.message_id:
        referenced = message.reference.resolved
        if referenced is None:
            try:
                referenced = await message.channel.fetch_message(message.reference.message_id)
            except (discord.NotFound, discord.Forbidden, discord.HTTPException):
                referenced = None

        if isinstance(referenced, discord.Message):
            original_text = referenced.content.strip()
            if not original_text:
                if referenced.attachments:
                    original_text = "[Attachment]"
                elif referenced.embeds:
                    original_text = "[Embed]"
                else:
                    original_text = "[No text]"

            if len(original_text) > 220:
                original_text = original_text[:217] + "..."

            embed_color = discord.Color(int("00DE9B", 16))
            color_hex = proxy_member.get("color") if proxy_member else None
            if color_hex:
                try:
                    embed_color = discord.Color(int(str(color_hex).lstrip("#"), 16))
                except (ValueError, TypeError):
                    pass

            reply_embed = discord.Embed(
                description=original_text,
                color=embed_color
            )
            reply_embed.set_author(name=f"Replying to {referenced.author.display_name}")
            reply_embed.add_field(name="Original", value=f"[Jump to message]({referenced.jump_url})", inline=False)

    guild_id = message.guild.id if message.guild else None
    server_appearance = get_server_appearance(system, guild_id)
    member_server_appearance = get_server_member_appearance(system, guild_id, proxy_member.get("id"))

    system_tag = (
        (member_server_appearance.get("system_tag") if member_server_appearance else None)
        or (server_appearance.get("system_tag") if server_appearance else None)
        or get_system_proxy_tag(system)
    )
    display_name = (
        (member_server_appearance.get("display_name") if member_server_appearance else None)
        or proxy_member.get("display_name")
        or proxy_member.get("name", "Unknown")
    )
    if system_tag:
        display_name = f"{display_name} {system_tag}"[:80]

    system_profile = get_system_profile(system)
    fallback_avatar = (
        (member_server_appearance.get("profile_pic") if member_server_appearance else None)
        or (server_appearance.get("profile_pic") if server_appearance else None)
        or system_profile.get("profile_pic")
    )

    send_kwargs = {
        "content": final_content,
        "username": display_name,
        "avatar_url": proxy_member.get("profile_pic") or fallback_avatar or None,
        "allowed_mentions": discord.AllowedMentions.none(),
        "wait": True,
    }

    if reply_embed is not None:
        send_kwargs["embeds"] = [reply_embed]

    if proxied_files:
        send_kwargs["files"] = proxied_files

    if isinstance(message.channel, discord.Thread):
        send_kwargs["thread"] = message.channel

    await webhook.send(**send_kwargs)

    # Update latch target only after a successful explicit proxy message.
    if explicit_proxy and latch_update:
        active_autoproxy_settings["latch_scope_id"] = scope_id_for_latch
        active_autoproxy_settings["latch_member_id"] = proxy_member.get("id")
        save_systems()

    try:
        await message.delete()
    except discord.Forbidden:
        pass
    except discord.HTTPException:
        pass

    await bot.process_commands(message)


@bot.event
async def on_message_edit(before: discord.Message, after: discord.Message):
    # Ignore edits that don't change content and ignore bot/webhook traffic.
    if after.author.bot or after.webhook_id is not None:
        return
    if before.content == after.content:
        return

    # Never proxy the starter post of a forum thread, even after edits.
    if isinstance(after.channel, discord.Thread):
        parent_channel = after.channel.parent
        if isinstance(parent_channel, discord.ForumChannel) and after.id == after.channel.id:
            return

    user_id = after.author.id
    system_id = get_user_system_id(user_id)
    if not system_id:
        return

    system = systems_data.get("systems", {}).get(system_id)
    if system is None:
        return

    active_autoproxy_settings, _autoproxy_scope = get_effective_autoproxy_settings(
        system,
        after.guild.id if after.guild else None,
    )

    explicit_proxy = False
    latch_update = False
    scope_id_for_latch = None
    proxy_member = None
    proxied_text = None

    if after.content.startswith(PROXY_PREFIX):
        explicit_proxy = True
        proxied_text = after.content[len(PROXY_PREFIX):].lstrip()
        scope_id_for_latch, proxy_member = get_fronting_member_for_user(after.author.id)
        if not proxy_member:
            await after.channel.send(
                f"{after.author.mention} no currently fronting member was found. Use /switchmember first."
            )
            return
        latch_update = True
    else:
        scope_id_for_latch, proxy_member, tagged_text = find_tagged_proxy_member(system, after.content)
        if proxy_member:
            explicit_proxy = True
            latch_update = True
            proxied_text = tagged_text

    if not explicit_proxy or not proxy_member:
        return

    proxied_files = []
    attachment_fallback_urls = []
    for attachment in after.attachments:
        try:
            proxied_files.append(await attachment.to_file())
        except (discord.HTTPException, discord.Forbidden):
            attachment_fallback_urls.append(attachment.url)

    if not proxied_text and not proxied_files and not attachment_fallback_urls:
        return

    webhook = await get_or_create_proxy_webhook(after.channel)
    if webhook is None:
        await after.channel.send("I can't proxy in this channel type.")
        return

    final_content = proxied_text
    if attachment_fallback_urls:
        attachments_text = "\n".join(attachment_fallback_urls)
        final_content = f"{proxied_text}\n{attachments_text}" if proxied_text else attachments_text

    reply_embed = None
    if after.reference and after.reference.message_id:
        referenced = after.reference.resolved
        if referenced is None:
            try:
                referenced = await after.channel.fetch_message(after.reference.message_id)
            except (discord.NotFound, discord.Forbidden, discord.HTTPException):
                referenced = None

        if isinstance(referenced, discord.Message):
            original_text = referenced.content.strip()
            if not original_text:
                if referenced.attachments:
                    original_text = "[Attachment]"
                elif referenced.embeds:
                    original_text = "[Embed]"
                else:
                    original_text = "[No text]"

            if len(original_text) > 220:
                original_text = original_text[:217] + "..."

            embed_color = discord.Color(int("00DE9B", 16))
            color_hex = proxy_member.get("color") if proxy_member else None
            if color_hex:
                try:
                    embed_color = discord.Color(int(str(color_hex).lstrip("#"), 16))
                except (ValueError, TypeError):
                    pass

            reply_embed = discord.Embed(description=original_text, color=embed_color)
            reply_embed.set_author(name=f"Replying to {referenced.author.display_name}")
            reply_embed.add_field(name="Original", value=f"[Jump to message]({referenced.jump_url})", inline=False)

    guild_id = after.guild.id if after.guild else None
    server_appearance = get_server_appearance(system, guild_id)
    member_server_appearance = get_server_member_appearance(system, guild_id, proxy_member.get("id"))

    system_tag = (
        (member_server_appearance.get("system_tag") if member_server_appearance else None)
        or (server_appearance.get("system_tag") if server_appearance else None)
        or get_system_proxy_tag(system)
    )
    display_name = (
        (member_server_appearance.get("display_name") if member_server_appearance else None)
        or proxy_member.get("display_name")
        or proxy_member.get("name", "Unknown")
    )
    if system_tag:
        display_name = f"{display_name} {system_tag}"[:80]

    system_profile = get_system_profile(system)
    fallback_avatar = (
        (member_server_appearance.get("profile_pic") if member_server_appearance else None)
        or (server_appearance.get("profile_pic") if server_appearance else None)
        or system_profile.get("profile_pic")
    )

    send_kwargs = {
        "content": final_content,
        "username": display_name,
        "avatar_url": proxy_member.get("profile_pic") or fallback_avatar or None,
        "allowed_mentions": discord.AllowedMentions.none(),
        "wait": True,
    }

    if reply_embed is not None:
        send_kwargs["embeds"] = [reply_embed]

    if proxied_files:
        send_kwargs["files"] = proxied_files

    if isinstance(after.channel, discord.Thread):
        send_kwargs["thread"] = after.channel

    await webhook.send(**send_kwargs)

    if explicit_proxy and latch_update:
        active_autoproxy_settings["latch_scope_id"] = scope_id_for_latch
        active_autoproxy_settings["latch_member_id"] = proxy_member.get("id")
        save_systems()

    try:
        await after.delete()
    except discord.Forbidden:
        pass
    except discord.HTTPException:
        pass

bot.run(TOKEN)
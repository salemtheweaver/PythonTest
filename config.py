# config.py — Constants, environment variables, and bot instance

import os
import re
import discord
from discord import app_commands
from discord.ext import commands
from dotenv import load_dotenv

# Load Discord bot token securely from environment variable
load_dotenv()
TOKEN = os.getenv("DISCORD_BOT_TOKEN")
if not TOKEN:
    raise RuntimeError("DISCORD_BOT_TOKEN environment variable not set.")

def _normalize_discord_id(raw_value: str) -> str | None:
    """Extract digits from a raw string to produce a clean Discord user ID."""
    value = (raw_value or "").strip()
    if not value:
        return None
    digits = "".join(ch for ch in value if ch.isdigit())
    return digits or None


def _parse_admin_user_ids(raw_value: str) -> set[str]:
    """Parse a comma/semicolon/whitespace-separated list of Discord user IDs."""
    tokens = re.split(r"[;,\s]+", raw_value or "")
    parsed = set()
    for token in tokens:
        user_id = _normalize_discord_id(token)
        if user_id:
            parsed.add(user_id)
    return parsed


ADMIN_USER_ID = _normalize_discord_id(os.getenv("CORTEX_ADMIN_USER_ID") or "")
ADMIN_USER_IDS = _parse_admin_user_ids(os.getenv("CORTEX_ADMIN_USER_IDS") or "")
# Permanent admin IDs that are always included regardless of env vars.
ADMIN_USER_IDS.add("247552740455219200")
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
GITHUB_REPO = os.getenv("GITHUB_REPO", "salemtheweaver/PythonTest")
INSTANCE_LABEL = (os.getenv("CORTEX_INSTANCE_LABEL") or "").strip()

JSON_FILE = "cortex_members.json"
TAGS_FILE = "tags.json"

# --- Proxy settings ---
PROXY_PREFIX = ";;"                     # Prefix to send as the currently fronting/latched member
PROXY_WEBHOOK_NAME = "Cortex Proxy"     # Name used for proxy webhook messages

# --- Rate limits ---
EXTERNAL_MSG_LIMIT_COUNT = 5            # Max external messages per user within the window
EXTERNAL_MSG_LIMIT_SECONDS = 60         # Rate limit window (seconds)
EXTERNAL_TARGET_LIMIT_SECONDS = 20      # Min seconds between messages to the same target
EXTERNAL_AUDIT_MAX = 200                # Max stored external audit log entries per system
MAX_PROXY_AUDIT_ENTRIES = 5000          # Max stored proxy message audit entries
ORIGIN_LOOKUP_EMOJIS = {"❓", "❔"}     # Reaction emojis that trigger proxy origin DMs

# Mutable runtime state
external_msg_rate_state = {}
external_target_rate_state = {}
PENDING_TIMEZONE_PROMPTS = {}
SCHEDULED_MESSAGES = {}  # {user_id: [{"send_at": datetime, "message": str}, ...]}
PROXY_MESSAGE_AUDIT = {}  # {proxied_message_id: metadata}

COMMON_TAG_PRESETS = [
    "host", "co-host", "primary", "protector", "persecutor",
    "gatekeeper", "caretaker", "trauma holder", "memory holder",
    "little", "middle", "teen", "adult",
    "fictive", "factive", "introject", "fragment", "subsystem",
    "fronting", "co-fronting", "social", "anxious", "nonverbal",
    "internal", "external",
]

MOD_COMMANDS = {
    "modreports", "modwarn", "modsuspend", "modban", "modunban",
}

ALLOWED_WHEN_BOT_BANNED = {"modappeal"}

SINGLET_ALLOWED_COMMANDS = {
    "register", "allowexternal", "externalprivacy", "externalstatus",
    "sendexternal", "blockuser", "unblockuser", "blockedusers",
    "externaltrustedonly", "trustuser", "untrustuser", "trustedusers",
    "frienduser", "unfrienduser", "friendusers",
    "muteuser", "unmuteuser", "mutedusers", "tempblockuser",
    "tempblockedusers", "externalpending", "approveexternal",
    "recentexternal", "externallimits", "externalquiethours",
    "externalretention", "checkin", "checkinstatus", "weeklymoodsummary",
    "settimezone", "timezonestatus", "refresh", "synccommands",
    "reportexternal", "setmode", "currentmode", "modestats",
    "systemprivacy", "alterprivacy", "privacystatus", "sendmessage",
    "members",
}

PROFILE_PRIVACY_LEVELS = {"private", "trusted", "friends", "public"}

TIMEZONE_ALIASES = {
    "EST": "America/New_York", "EDT": "America/New_York",
    "CST": "America/Chicago", "CDT": "America/Chicago",
    "MST": "America/Denver", "MDT": "America/Denver",
    "PST": "America/Los_Angeles", "PDT": "America/Los_Angeles",
    "AKST": "America/Anchorage", "AKDT": "America/Anchorage",
    "HST": "Pacific/Honolulu",
    "GMT": "Etc/GMT", "BST": "Europe/London",
    "CET": "Europe/Paris", "CEST": "Europe/Paris",
    "EET": "Europe/Athens",
    "JST": "Asia/Tokyo", "KST": "Asia/Seoul",
    "IST": "Asia/Kolkata",
    "AEST": "Australia/Sydney", "AEDT": "Australia/Sydney",
    "UTC": "UTC",
}

COMMON_TIMEZONES = [
    "UTC", "America/New_York", "America/Chicago", "America/Denver",
    "America/Los_Angeles", "America/Anchorage", "Pacific/Honolulu",
    "Europe/London", "Europe/Paris", "Europe/Berlin", "Europe/Athens",
    "Asia/Tokyo", "Asia/Seoul", "Asia/Kolkata", "Asia/Singapore",
    "Australia/Sydney", "Australia/Perth",
]

TIMEZONE_FIXED_OFFSETS = {
    # Only non-IANA names that ZoneInfo may not recognize.
    # IANA names (America/New_York, etc.) are handled by ZoneInfo with proper DST.
    "UTC": 0, "Etc/GMT": 0,
}

DEFAULT_FOCUS_MODES = [
    "studying", "gaming", "social", "burnout", "rest",
    "creative", "work", "exercise", "errands",
]


def with_instance_label(message: str) -> str:
    """Prefix a message with the instance label (for multi-instance deployments)."""
    if not INSTANCE_LABEL:
        return message
    return f"[{INSTANCE_LABEL}] {message}"


# --- Bot instance setup ---

intents = discord.Intents.default()
intents.message_content = True
intents.members = True
intents.guilds = True
intents.reactions = True
intents.presences = True


# CortexCommandTree is defined here but its interaction_check references
# helpers that will be patched in at startup by cortex.py
class CortexCommandTree(discord.app_commands.CommandTree):
    """Custom command tree whose interaction_check is monkey-patched at startup by cortex.py."""

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        return True


bot = commands.Bot(
    command_prefix=commands.when_mentioned_or("Cor;", "cor;"),
    intents=intents,
    help_command=None,
    tree_cls=CortexCommandTree,
)
tree = bot.tree

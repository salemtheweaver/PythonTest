# helpers.py — Helper/utility functions extracted from cortex.py

import os
import json
import re
import asyncio
import base64
import urllib.request
import urllib.error
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
from copy import deepcopy

import discord
from discord import app_commands
from discord.ext import commands

from config import (
    ADMIN_USER_ID,
    ADMIN_USER_IDS,
    COMMON_TAG_PRESETS,
    EXTERNAL_MSG_LIMIT_COUNT,
    EXTERNAL_MSG_LIMIT_SECONDS,
    EXTERNAL_TARGET_LIMIT_SECONDS,
    EXTERNAL_AUDIT_MAX,
    PENDING_TIMEZONE_PROMPTS,
    SCHEDULED_MESSAGES,
    PROXY_MESSAGE_AUDIT,
    MAX_PROXY_AUDIT_ENTRIES,
    ORIGIN_LOOKUP_EMOJIS,
    PROXY_WEBHOOK_NAME,
    PROXY_PREFIX,
    TIMEZONE_ALIASES,
    COMMON_TIMEZONES,
    TIMEZONE_FIXED_OFFSETS,
    PROFILE_PRIVACY_LEVELS,
    DEFAULT_FOCUS_MODES,
    with_instance_label,
    bot,
    tree,
    external_msg_rate_state,
    external_target_rate_state,
    MOD_COMMANDS,
    ALLOWED_WHEN_BOT_BANNED,
    SINGLET_ALLOWED_COMMANDS,
    JSON_FILE,
)

from data import systems_data, save_systems

# Legacy compatibility stub — the old flat members dict is no longer used for
# new code, but start_front / end_front still reference it as a default arg.
members = {}


# ---------------------------------------------------------------------------
# Moderation helpers
# ---------------------------------------------------------------------------

def get_moderation_state():
    state = systems_data.setdefault("_moderation", {})
    state.setdefault("reports", [])
    state.setdefault("sanctions", {})
    state.setdefault("events", [])
    state.setdefault("bot_admin_user_ids", [])
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
    normalized_user_id = str(user_id)

    if ADMIN_USER_ID and str(user_id) == str(ADMIN_USER_ID):
        return True
    if normalized_user_id in ADMIN_USER_IDS:
        return True

    # Fallback: allow runtime/persisted admin IDs stored in moderation state.
    state = get_moderation_state()
    persisted_admin_ids = state.get("bot_admin_user_ids", [])
    if normalized_user_id in {str(admin_id) for admin_id in persisted_admin_ids}:
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


# ---------------------------------------------------------------------------
# System / member lookup helpers
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Command gate checks
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Member scope helpers
# ---------------------------------------------------------------------------

def get_system_members(system_id, subsystem_id=None):
    if system_id not in systems_data["systems"]:
        return None

    system = systems_data["systems"][system_id]

    if subsystem_id is None:
        return system.get("members", {})
    else:
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
        members_dict = system.get("subsystems", {}).get(subsystem_id, {}).get("members")
        if members_dict is None:
            return None, None, None, None, f"Member not found in {get_scope_label(subsystem_id)}."
        resolved_id, resolved_member, err = resolve_member_identifier(members_dict, raw)
        if err:
            return None, None, None, None, err
        return subsystem_id, members_dict, resolved_id, resolved_member, None

    candidates = []
    needle = raw.lower()

    for scope_id, members_dict in iter_system_member_dicts(system):
        for mid, member in members_dict.items():
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
                candidates.append((rank, scope_id, members_dict, mid, member))

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
        _, scope_id, members_dict, mid, member = deduped[0]
        return scope_id, members_dict, mid, member, None

    labels = [f"{item[4].get('name', item[3])} (`{item[3]}` in {get_scope_label(item[1])})" for item in deduped[:6]]
    suffix = " ..." if len(deduped) > 6 else ""
    return None, None, None, None, "Ambiguous member name. Matches: " + ", ".join(labels) + suffix + "\nHint: use full name or member ID."


# ---------------------------------------------------------------------------
# Group helpers
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Member ID helpers
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Migration helpers
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Fronting / autoproxy helpers
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# System tag / proxy tag helpers
# ---------------------------------------------------------------------------

def get_system_proxy_tag(system):
    """Return system-level tag suffix for proxied messages."""
    return system.get("system_tag")


def normalize_tag_value(tag):
    return str(tag or "").strip().lower()


def get_system_tag_list(system, create=False):
    if not isinstance(system, dict):
        return []
    existing = system.get("custom_tags")
    if not isinstance(existing, list):
        if not create:
            return []
        existing = []
        system["custom_tags"] = existing

    normalized = []
    seen = set()
    for raw in existing:
        cleaned = normalize_tag_value(raw)
        if not cleaned or cleaned in seen:
            continue
        seen.add(cleaned)
        normalized.append(cleaned)

    if create:
        system["custom_tags"] = normalized
    return normalized


def get_available_tags_for_system(system):
    available = []
    seen = set()
    for raw in COMMON_TAG_PRESETS + get_system_tag_list(system, create=False):
        cleaned = normalize_tag_value(raw)
        if not cleaned or cleaned in seen:
            continue
        seen.add(cleaned)
        available.append(cleaned)
    return available


def add_custom_tags_to_system(system, tags):
    custom = get_system_tag_list(system, create=True)
    common = set(COMMON_TAG_PRESETS)
    changed = False
    for raw in tags or []:
        cleaned = normalize_tag_value(raw)
        if not cleaned or cleaned in common or cleaned in custom:
            continue
        custom.append(cleaned)
        changed = True
    if changed:
        system["custom_tags"] = custom
    return changed


# ---------------------------------------------------------------------------
# System profile / appearance helpers
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Front reminder / timezone helpers
# ---------------------------------------------------------------------------

def get_front_reminder_settings(system):
    """Return and initialize DM front reminder settings for a system."""
    settings = system.setdefault("front_reminders", {})
    settings.setdefault("enabled", False)
    settings.setdefault("hours", 4)
    return settings


def get_birthday_reminder_settings(system):
    """Return and initialize DM birthday reminder settings for a system."""
    settings = system.setdefault("birthday_reminders", {})
    settings.setdefault("enabled", True)
    settings.setdefault("days_before", [2, 1])
    settings.setdefault("sent_keys", {})
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


# ---------------------------------------------------------------------------
# Check-in / wellness helpers
# ---------------------------------------------------------------------------

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
        return "\U0001f7e5"
    if rating <= 4:
        return "\U0001f7e7"
    if rating <= 6:
        return "\U0001f7e8"
    if rating <= 8:
        return "\U0001f7e9"
    return "\U0001f7e6"

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


# ---------------------------------------------------------------------------
# Focus Mode helpers (singlet)
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Embed / display helpers
# ---------------------------------------------------------------------------

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
    """Build a member profile embed matching PluralKit-style layout."""
    try:
        embed_color = int(str(member.get("color", "FFFFFF")).lstrip("#"), 16)
    except (TypeError, ValueError):
        embed_color = int("00DE9B", 16)

    def _truncate(text, limit):
        text = str(text or "").strip()
        if len(text) <= limit:
            return text
        return text[:limit - 3].rstrip() + "..."

    display_name = str(member.get("display_name") or "").strip() or None
    member_name = member["name"]

    # Build author line: "MemberName (SystemName)" like PK
    author_name = member_name
    if system:
        system_name = system.get("system_name") or system.get("name")
        if system_name:
            author_name = f"{member_name} ({system_name})"

    profile_pic_url = normalize_embed_image_url(member.get("profile_pic"))

    embed = discord.Embed(color=embed_color)
    author_kwargs = {"name": author_name}
    if profile_pic_url:
        author_kwargs["icon_url"] = profile_pic_url
    embed.set_author(**author_kwargs)

    if profile_pic_url:
        embed.set_thumbnail(url=profile_pic_url)

    banner_url = normalize_embed_image_url(member.get("banner"))
    if banner_url:
        embed.set_image(url=banner_url)

    # --- Section 1: Basic info as embed description ---
    info_lines = []
    if display_name and display_name != member_name:
        info_lines.append(f"**Display name:** {_truncate(display_name, 250)}")
    pronouns = member.get("pronouns")
    if pronouns:
        info_lines.append(f"**Pronouns:** {_truncate(pronouns, 250)}")
    birthday = member.get("birthday")
    if birthday:
        # Format birthday as MM/DD regardless of stored format
        try:
            parts = str(birthday).strip().split("-")
            if len(parts) == 3:  # YYYY-MM-DD
                birthday_display = f"{parts[1]}/{parts[2]}"
            elif len(parts) == 2:  # MM-DD
                birthday_display = f"{parts[0]}/{parts[1]}"
            else:
                birthday_display = birthday
        except Exception:
            birthday_display = birthday
        info_lines.append(f"**Birthday:** {birthday_display}")
    tags = ", ".join(member.get("tags", [])) if member.get("tags") else None
    if tags:
        info_lines.append(f"**Tags:** {_truncate(tags, 250)}")
    playlist_text = format_playlist_link(member["yt_playlist"]) if member.get("yt_playlist") else None
    if playlist_text:
        info_lines.append(f"**Playlist:** {playlist_text}")

    # Fronting status
    current_front = member.get("current_front")
    if current_front:
        try:
            start_dt = datetime.fromisoformat(current_front["start"])
            duration = format_duration((datetime.now(timezone.utc) - start_dt).total_seconds())
            cofront_ids = current_front.get("cofronts", [])
            cofront_text = ""
            if cofront_ids and system:
                cofront_names = []
                for scope_id, members_dict in iter_system_member_dicts(system):
                    for cofront_id in cofront_ids:
                        if cofront_id in members_dict:
                            cofront_names.append(members_dict[cofront_id].get("name", cofront_id))
                if cofront_names:
                    cofront_text = f" (with {', '.join(cofront_names)})"
            info_lines.append(f"**Currently fronting:** Yes, for {duration}{cofront_text}")
        except (ValueError, KeyError):
            info_lines.append("**Currently fronting:** Yes")
    else:
        info_lines.append("**Currently fronting:** No")

    total_front_seconds = calculate_front_duration(member)
    if total_front_seconds > 0:
        info_lines.append(f"**Total front time:** {format_duration(total_front_seconds)}")

    color_val = member.get("color")
    if color_val:
        info_lines.append(f"**Color:** #{str(color_val).lstrip('#')}")

    if info_lines:
        embed.description = "\n".join(info_lines)

    # --- Section 2: Proxy tags (field with dash separator) ---
    proxy_text = render_member_proxy_result(member)
    if proxy_text and proxy_text != "Not set":
        embed.add_field(
            name="Proxy tags",
            value=f"`{_truncate(proxy_text, 1000)}`",
            inline=False,
        )

    # --- Section 3: Groups ---
    if system is not None:
        groups_text = format_member_group_lines(system, member)
        if groups_text and groups_text != "None":
            embed.add_field(
                name="Groups",
                value=_truncate(groups_text, 1024),
                inline=False,
            )

    # --- Section 4: Description/bio ---
    bio_text = str(member.get("description") or "").strip()
    if bio_text:
        lines = [line.strip() for line in bio_text.split('\n')]
        bio_text = '\n'.join(line for line in lines if line)
        embed.add_field(
            name="\u200b",
            value=_truncate(bio_text, 1024),
            inline=False,
        )

    # Footer: Member ID + Created date (PK style)
    footer_parts = [f"Member ID: {member['id']}"]
    created_iso = member.get("created_at")
    if created_iso:
        try:
            created_dt = datetime.fromisoformat(created_iso)
            footer_parts.append(f"Created on {created_dt.strftime('%B %d, %Y')}")
        except Exception:
            pass
    embed.set_footer(text=" | ".join(footer_parts))

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


# ---------------------------------------------------------------------------
# External messaging helpers
# ---------------------------------------------------------------------------

def get_external_settings(system):
    """Return and initialize external messaging settings for a system."""
    settings = system.setdefault("external_messages", {})
    settings.setdefault("accept", False)
    settings.setdefault("blocked_users", [])
    settings.setdefault("muted_users", [])
    settings.setdefault("trusted_only", False)
    settings.setdefault("trusted_users", [])
    settings.setdefault("friend_users", [])
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
    settings["friend_users"] = [str(uid) for uid in settings.get("friend_users", [])]
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


# ---------------------------------------------------------------------------
# Privacy helpers
# ---------------------------------------------------------------------------

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
    settings = get_external_settings(system)
    trusted = set(str(uid) for uid in settings.get("trusted_users", []))
    friends = set(str(uid) for uid in settings.get("friend_users", []))
    if level == "friends":
        return viewer_id in trusted or viewer_id in friends
    if level == "trusted":
        return viewer_id in trusted
    return False


def can_view_system_data(system, viewer_user_id):
    return can_view_with_privacy(get_system_privacy_level(system), system, viewer_user_id)


def can_view_member_data(system, member, viewer_user_id):
    return can_view_with_privacy(get_member_privacy_level(member), system, viewer_user_id)


def resolve_target_system_for_view(requester_user_id, target_user_id_raw=None):
    if target_user_id_raw is None:
        target_user_id = str(requester_user_id)
        system_id = get_user_system_id(target_user_id)
        if not system_id:
            return None, None, None, with_instance_label("You do not have a registered system. Use /register.")
    else:
        parsed = parse_discord_user_id(target_user_id_raw)
        if not parsed:
            return None, None, None, with_instance_label("Invalid target user ID. Use a numeric Discord ID or mention.")
        target_user_id = parsed
        system_id = get_user_system_id(target_user_id)
        if not system_id:
            return None, None, None, with_instance_label("Target user does not have a registered system.")

    system = systems_data.get("systems", {}).get(system_id)
    if not system:
        return None, None, None, with_instance_label("System not found.")

    return system_id, system, target_user_id, None


# ---------------------------------------------------------------------------
# Time / scheduling helpers
# ---------------------------------------------------------------------------

def parse_time_delta(time_str):
        """Parse friendly duration strings and return a timedelta or None.

        Accepts examples like `2hrs`, `30 mins`, `1 day`, `1h30m`, `1d 2h 15m`,
        `90m`, `tomorrow`, and `next day`.
        """
        if not time_str:
            return None

        normalized = time_str.strip().lower()
        if not normalized:
            return None

        alias_map = {
            "tomorrow": timedelta(days=1),
            "next day": timedelta(days=1),
            "nextday": timedelta(days=1),
        }
        if normalized in alias_map:
            return alias_map[normalized]

        compact = re.sub(r'\s+', '', normalized)
        unit_map = {
            'd': 'days',
            'day': 'days',
            'days': 'days',
            'h': 'hours',
            'hr': 'hours',
            'hrs': 'hours',
            'hour': 'hours',
            'hours': 'hours',
            'm': 'minutes',
            'min': 'minutes',
            'mins': 'minutes',
            'minute': 'minutes',
            'minutes': 'minutes',
            's': 'seconds',
            'sec': 'seconds',
            'secs': 'seconds',
            'second': 'seconds',
            'seconds': 'seconds',
        }

        total_kwargs = {
            'days': 0,
            'hours': 0,
            'minutes': 0,
            'seconds': 0,
        }

        matches = list(re.finditer(r'(\d+)\s*(days?|d|hours?|hrs?|h|minutes?|mins?|min|m|seconds?|secs?|sec|s)', compact))
        if not matches:
            return None

        consumed = ''.join(match.group(0) for match in matches)
        if consumed != compact:
            return None

        for match in matches:
            value = int(match.group(1))
            canonical_unit = unit_map[match.group(2)]
            total_kwargs[canonical_unit] += value

        total_delta = timedelta(**total_kwargs)
        if total_delta.total_seconds() <= 0:
            return None
        return total_delta


def parse_sendmessage_args(args_str):
    """Parse 'target:future time:2hrs message:\"your message here\"' format.
    Returns (target, time_delta, message, error_msg)
    """
    if not args_str:
        return None, None, None, "No arguments provided."

    parts = {}
    key_matches = list(re.finditer(r'(\w+):', args_str))
    for index, key_match in enumerate(key_matches):
        key = key_match.group(1).lower()
        value_start = key_match.end()
        value_end = key_matches[index + 1].start() if index + 1 < len(key_matches) else len(args_str)
        value = args_str[value_start:value_end].strip()
        if len(value) >= 2 and value[0] == '"' and value[-1] == '"':
            value = value[1:-1]
        parts[key] = value

    # Validate required fields
    target = parts.get('target', '').lower()
    if target != 'future':
        return None, None, None, "Target must be 'future' (currently only self-messaging supported)."

    time_str = parts.get('time', '')
    if not time_str:
        return None, None, None, "Missing 'time' parameter (e.g., time:2hrs or time:\"1h 30m\")."

    time_delta = parse_time_delta(time_str)
    if not time_delta:
        return None, None, None, (
            f"Invalid time format: {time_str}. Use formats like 2hrs, 30mins, 1day, 1h30m, 1d 2h, or tomorrow."
        )

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


# ---------------------------------------------------------------------------
# PluralKit import helpers
# ---------------------------------------------------------------------------

def normalize_pluralkit_token(raw_token: str) -> str:
    """Normalize a pasted PluralKit token from common copy/paste wrappers."""
    token = str(raw_token or "").strip()
    if not token:
        return ""

    # Accept tokens pasted with an "Authorization:" prefix.
    if token.lower().startswith("authorization:"):
        token = token.split(":", 1)[1].strip()

    # Some users paste a bearer-form token from generic API tools.
    if token.lower().startswith("bearer "):
        token = token[7:].strip()

    # Remove matching quote wrappers repeatedly.
    while len(token) >= 2 and token[0] == token[-1] and token[0] in {"`", '"', "'"}:
        token = token[1:-1].strip()

    # Remove Discord spoiler wrapper if copied from ||token|| formatting.
    if token.startswith("||") and token.endswith("||") and len(token) > 4:
        token = token[2:-2].strip()

    # Remove angle-bracket wrappers and any embedded whitespace/control chars.
    if token.startswith("<") and token.endswith(">") and len(token) > 2:
        token = token[1:-1].strip()

    token = "".join(ch for ch in token if ch.isprintable() and not ch.isspace())

    return token

def _fetch_pluralkit_members_sync(token: str):
    """Fetch members for the authenticated PluralKit system."""
    token = normalize_pluralkit_token(token)
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


def _fetch_pluralkit_system_sync(token: str):
    """Fetch system details for the authenticated PluralKit system."""
    token = normalize_pluralkit_token(token)
    request = urllib.request.Request(
        "https://api.pluralkit.me/v2/systems/@me",
        headers={
            "Authorization": token,
            "User-Agent": "CortexBot/1.0 (+importpluralkit)"
        },
        method="GET",
    )

    with urllib.request.urlopen(request, timeout=20) as response:
        payload = response.read().decode("utf-8")
        data = json.loads(payload)
        if not isinstance(data, dict):
            raise ValueError("Unexpected PluralKit system response format.")
        return data


async def fetch_pluralkit_members(token: str):
    """Async wrapper for PluralKit member fetch."""
    return await asyncio.to_thread(_fetch_pluralkit_members_sync, token)


async def fetch_pluralkit_system(token: str):
    """Async wrapper for PluralKit system fetch."""
    return await asyncio.to_thread(_fetch_pluralkit_system_sync, token)


# ---------------------------------------------------------------------------
# Proxy format helpers
# ---------------------------------------------------------------------------

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


def normalize_hex(hex_code: str):
    if not hex_code:
        return None
    hex_code = hex_code.strip().lstrip("#")
    if len(hex_code) != 6 or any(c not in "0123456789ABCDEFabcdef" for c in hex_code):
        raise ValueError("Color must be a 6-digit HEX code (e.g., FF0000)")
    return hex_code.upper()


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


# ---------------------------------------------------------------------------
# External messaging target resolution
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Proxy tag matching / management
# ---------------------------------------------------------------------------

def find_tagged_proxy_member(system, content):
    """Find the best matching member proxy tag and return (scope_id, member, stripped_text)."""
    best_match = None

    def _normalize_proxy_tag_text(text: str) -> str:
        # Ignore emoji presentation/joiner characters so equivalent emoji tags still match.
        return "".join(ch for ch in (text or "") if ch not in {"\ufe0e", "\ufe0f", "\u200d"})

    def _match_prefix_end_index(text: str, prefix: str):
        if not prefix:
            return 0
        if text.startswith(prefix):
            return len(prefix)

        # Support case-insensitive matching for text-like prefixes.
        if any(ch.isalpha() for ch in prefix) and text[:len(prefix)].casefold() == prefix.casefold():
            return len(prefix)

        normalized_prefix = _normalize_proxy_tag_text(prefix)
        if not normalized_prefix:
            return 0

        consumed = []
        index = 0
        for ch in text:
            index += 1
            if ch in {"\ufe0e", "\ufe0f", "\u200d"}:
                continue
            consumed.append(ch)
            consumed_len = len(consumed)
            if normalized_prefix[:consumed_len] != "".join(consumed):
                return None
            if consumed_len == len(normalized_prefix):
                return index
        return None

    def _match_suffix_start_index(text: str, suffix: str):
        if not suffix:
            return len(text)
        if text.endswith(suffix):
            return len(text) - len(suffix)

        # Support case-insensitive matching for text-like suffixes.
        if any(ch.isalpha() for ch in suffix) and text[-len(suffix):].casefold() == suffix.casefold():
            return len(text) - len(suffix)

        normalized_suffix = _normalize_proxy_tag_text(suffix)
        if not normalized_suffix:
            return len(text)

        i = len(text) - 1
        j = len(normalized_suffix) - 1
        start = len(text)
        while i >= 0 and j >= 0:
            ch = text[i]
            if ch in {"\ufe0e", "\ufe0f", "\u200d"}:
                i -= 1
                continue
            if ch != normalized_suffix[j]:
                return None
            start = i
            i -= 1
            j -= 1

        if j >= 0:
            return None
        return start

    for scope_id, members_dict in iter_system_member_dicts(system):
        for member_data in members_dict.values():
            for proxy_format in get_member_proxy_formats(member_data):
                prefix = proxy_format.get("prefix") or ""
                suffix = proxy_format.get("suffix") or ""
                prefix_end = _match_prefix_end_index(content, prefix)
                if prefix_end is None:
                    continue
                suffix_start = _match_suffix_start_index(content, suffix)
                if suffix_start is None:
                    continue
                if suffix_start < prefix_end:
                    continue

                match_len = len(_normalize_proxy_tag_text(prefix)) + len(_normalize_proxy_tag_text(suffix))
                # Prefer the most specific (longest) tag format to avoid collisions.
                if best_match is None or match_len > best_match[0]:
                    stripped = content[prefix_end:suffix_start].strip()
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


# ---------------------------------------------------------------------------
# Webhook / proxy origin helpers
# ---------------------------------------------------------------------------

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


def remember_proxied_message_origin(proxied_message, source_message, sender_user_id, system_id, scope_id, member_data):
    """Store lightweight origin metadata for a proxied webhook message."""
    if proxied_message is None:
        return

    message_id = int(proxied_message.id)
    PROXY_MESSAGE_AUDIT[message_id] = {
        "proxied_message_id": message_id,
        "proxied_channel_id": int(proxied_message.channel.id),
        "source_message_id": int(source_message.id) if source_message else None,
        "sender_user_id": int(sender_user_id),
        "system_id": str(system_id) if system_id is not None else None,
        "scope_id": str(scope_id) if scope_id is not None else None,
        "member_id": str(member_data.get("id")) if isinstance(member_data, dict) and member_data.get("id") is not None else None,
        "member_name": (member_data.get("name") if isinstance(member_data, dict) else None) or "Unknown",
        "created_at": datetime.now(timezone.utc).isoformat(),
    }

    if len(PROXY_MESSAGE_AUDIT) > MAX_PROXY_AUDIT_ENTRIES:
        oldest_key = next(iter(PROXY_MESSAGE_AUDIT))
        PROXY_MESSAGE_AUDIT.pop(oldest_key, None)


async def send_proxy_origin_dm(reactor_user, audit_entry):
    """DM a user with proxy origin info while respecting member privacy."""
    raw_sender_id = audit_entry.get("sender_user_id")
    if raw_sender_id is None:
        await reactor_user.send("Origin info is unavailable for this message.")
        return
    sender_user_id = int(raw_sender_id)
    sender_mention = f"<@{sender_user_id}>"

    lines = [
        "You requested origin info for a proxied message.",
        f"Sender account: {sender_mention} (ID `{sender_user_id}`)",
    ]

    system_id = audit_entry.get("system_id")
    scope_id = audit_entry.get("scope_id")
    member_id = audit_entry.get("member_id")
    member_name = audit_entry.get("member_name") or "Unknown"

    system = systems_data.get("systems", {}).get(str(system_id)) if system_id else None
    member = None
    if system and member_id:
        resolved_scope = None if scope_id in {None, "", "None"} else scope_id
        member = get_member_from_scope(system, resolved_scope, member_id)

    if system and member and can_view_member_data(system, member, reactor_user.id):
        display_name = member.get("name", member_name)
        lines.append(f"Proxied member: **{display_name}** (ID `{member_id}` in {get_scope_label(scope_id if scope_id not in {None, '', 'None'} else None)})")
    elif system and member:
        lines.append("Proxied member: hidden by privacy settings.")

    # Show the member card when the viewer is allowed by member privacy rules.
    if system and member and can_view_member_data(system, member, reactor_user.id):
        try:
            embed = build_member_profile_embed(member, system=system)
            await reactor_user.send("\n".join(lines), embed=embed)
            return
        except Exception:
            # Fall back to text-only DM if embed rendering/sending fails.
            pass

    await reactor_user.send("\n".join(lines))


# ---------------------------------------------------------------------------
# Autocomplete functions
# ---------------------------------------------------------------------------

async def member_name_autocomplete(interaction: discord.Interaction, current: str):
    try:
        user_id = interaction.user.id
        system_id = get_user_system_id(user_id)
        if not system_id:
            return []

        subsystem_id = interaction.namespace.subsystem_id if hasattr(interaction.namespace, "subsystem_id") else None
        members_dict = get_system_members(system_id, subsystem_id)
        if not members_dict:
            return []

        options = []
        query = (current or "").lower()
        for member_id, member_data in members_dict.items():
            member_name = member_data.get("name", "")
            if query in member_name.lower() or query in str(member_id).lower():
                options.append(app_commands.Choice(name=f"{member_name} ({member_id})", value=member_name))
            if len(options) >= 25:
                break
        return options
    except Exception:
        return []

async def member_id_autocomplete(interaction: discord.Interaction, current: str):
    try:
        user_id = interaction.user.id
        system_id = get_user_system_id(user_id)
        if not system_id:
            return []
        subsystem_id = interaction.namespace.subsystem_id if hasattr(interaction.namespace, "subsystem_id") else None
        members_dict = get_system_members(system_id, subsystem_id)
        if not members_dict:
            return []
        options = []
        query = (current or "").lower()
        for member_id, member_data in members_dict.items():
            member_name = member_data.get("name", "")
            if query in member_id.lower() or query in member_name.lower():
                options.append(app_commands.Choice(name=f"{member_name} ({member_id})", value=member_id))
            if len(options) >= 25:
                break
        return options
    except Exception:
        return []

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
        user_id = interaction.user.id
        system_id = get_user_system_id(user_id)
        if not system_id:
            return []
        system = systems_data.get("systems", {}).get(system_id)
        if not system:
            return []

        options = []
        query = (current or "").lower()
        for tag in get_available_tags_for_system(system):
            if query in tag.lower():
                options.append(app_commands.Choice(name=tag, value=tag))
            if len(options) >= 25:
                break
        return options
    except Exception:
        return []


# Tag views are defined in views.py — imported from there by commands_slash.py and commands_prefix.py


# ---------------------------------------------------------------------------
# Fronting helpers (unified)
# ---------------------------------------------------------------------------

def format_us(iso_string):
    """Format an ISO datetime string to US-friendly format."""
    try:
        dt = datetime.fromisoformat(iso_string)
        return dt.strftime("%m/%d/%Y %I:%M %p")
    except Exception:
        return iso_string

def start_front(member_id, cofronts=None, members_dict=None, persist=True):
    """Start fronting for a member with optional co-fronts."""
    if cofronts is None:
        cofronts = []
    if members_dict is None:
        members_dict = members

    member = members_dict[member_id]
    now_iso = datetime.now(timezone.utc).isoformat()

    # End any existing front
    if member.get("current_front"):
        end_front(member_id, members_dict=members_dict, persist=False)

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
    if persist:
        save_systems()

def end_front(member_id, members_dict=None, persist=True):
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
    if persist:
        save_systems()


# ---------------------------------------------------------------------------
# Front duration helpers (unified)
# ---------------------------------------------------------------------------

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

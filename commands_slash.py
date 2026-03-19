import discord
from discord import app_commands
from discord.ext import commands
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
import asyncio
import json
import os
import random
from copy import deepcopy
import urllib.request
import urllib.error

from config import (
    bot, tree, JSON_FILE, TAGS_FILE, EXTERNAL_TARGET_LIMIT_SECONDS,
    PROXY_PREFIX, COMMON_TAG_PRESETS, DEFAULT_FOCUS_MODES,
    PENDING_TIMEZONE_PROMPTS, SCHEDULED_MESSAGES,
    with_instance_label,
)
from data import systems_data, save_systems

from helpers import (
    get_user_system_id,
    get_system_members,
    get_scope_label,
    get_system_profile,
    get_system_proxy_tag,
    get_external_settings,
    get_autoproxy_settings,
    get_server_autoproxy_settings,
    get_effective_autoproxy_settings,
    apply_autoproxy_mode,
    get_front_reminder_settings,
    get_system_timezone_name,
    normalize_timezone_name,
    get_system_timezone,
    timezone_autocomplete,
    get_checkin_settings,
    get_checkin_trend_text,
    build_weekly_checkin_summary,
    current_week_key,
    get_focus_modes,
    get_all_mode_names,
    start_focus_mode,
    end_focus_mode,
    calc_mode_stats,
    normalize_hex,
    build_member_profile_embed,
    build_subsystem_card_embed,
    get_system_tag_list,
    get_available_tags_for_system,
    add_custom_tags_to_system,
    normalize_tag_value,
    member_name_autocomplete,
    member_id_autocomplete,
    switchmember_member_id_autocomplete,
    subsystem_id_autocomplete,
    tag_autocomplete,
    resolve_member_identifier,
    resolve_member_identifier_in_system,
    get_member_proxy_formats,
    set_member_proxy_formats,
    add_member_proxy_format,
    remove_member_proxy_format,
    render_member_proxy_format,
    render_member_proxy_result,
    parse_proxy_format_with_placeholder,
    normalize_proxy_formats,
    format_member_group_lines,
    get_group_settings,
    get_next_group_id,
    get_group_path_text,
    sorted_group_ids_for_member,
    get_next_system_member_id,
    start_front,
    end_front,
    calculate_front_duration,
    format_duration,
    time_of_day_bucket,
    iter_system_member_dicts,
    get_member_sort_mode,
    sort_member_rows,
    get_member_lookup_keys,
    format_playlist_link,
    normalize_embed_image_url,
    is_ephemeral_discord_attachment_url,
    get_fronting_member_for_user,
    find_tagged_proxy_member,
    get_member_proxy_parts,
    move_member_between_scopes,
    resolve_target_member_scope,
    get_member_from_scope,
    parse_discord_user_id,
    parse_bool_token,
    get_user_mode,
    ensure_moderator,
    is_bot_moderator_user,
    get_moderation_state,
    get_user_sanctions,
    add_mod_event,
    check_and_update_external_rate_limit,
    check_and_update_external_target_rate_limit,
    format_inbox_entry_for_channel,
    format_inbox_entry_for_dm,
    is_in_quiet_hours,
    prune_external_temp_blocks,
    cleanup_external_inbox_entries,
    add_external_audit_entry,
    parse_time_delta,
    parse_sendmessage_args,
    add_scheduled_message,
    get_server_appearance,
    get_server_member_appearance,
    resolve_member_for_override,
    migrate_system_member_ids,
    get_migrations_state,
    normalize_profile_privacy_level,
    get_system_privacy_level,
    get_member_privacy_level,
    can_view_system_data,
    can_view_member_data,
    resolve_target_system_for_view,
    fetch_pluralkit_members,
    fetch_pluralkit_system,
    normalize_pluralkit_token,
    map_pluralkit_member_to_cortex,
    is_user_suspended,
    _recent_checkins,
)
from data import _github_save_file
from config import (
    TIMEZONE_FIXED_OFFSETS,
    COMMON_TIMEZONES,
    EXTERNAL_MSG_LIMIT_COUNT,
    EXTERNAL_MSG_LIMIT_SECONDS,
)
from views import GroupOrderView


# =============================================
# Local helpers used only within this file
# =============================================

# Legacy members dict (loaded from JSON_FILE for backwards compat)
if os.path.exists(JSON_FILE):
    with open(JSON_FILE, "r") as f:
        members = json.load(f)
else:
    members = {}


def save_members():
    with open(JSON_FILE, "w") as f:
        json.dump(members, f, indent=4)
    _github_save_file(JSON_FILE, members)


def get_next_member_id():
    if not members:
        return 1
    return max(int(mid) for mid in members.keys()) + 1


def format_us(iso_string):
    """Format an ISO datetime string to US-friendly format."""
    try:
        dt = datetime.fromisoformat(iso_string)
        return dt.strftime("%m/%d/%Y %I:%M %p")
    except Exception:
        return iso_string


# =============================================
# Tag selection views
# =============================================
class TagSelect(discord.ui.Select):
    def __init__(self, available_tags, preselected=None):
        options = [
            discord.SelectOption(label=tag, value=tag, default=(tag in preselected if preselected else False))
            for tag in available_tags
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
    def __init__(self, available_tags, preselected=None):
        super().__init__(timeout=120)
        self.selected_tags = preselected or []
        if available_tags:
            self.add_item(TagSelect(available_tags, preselected))
        self.add_item(ConfirmTags())

class TagMultiSelect(discord.ui.Select):
    def __init__(self, available_tags, members_dict):
        self.members_dict = members_dict
        options = [discord.SelectOption(label=tag, value=tag) for tag in available_tags[:25]]
        super().__init__(
            placeholder="Select one or more tags (up to 25 shown)...",
            min_values=1,
            max_values=len(options) if options else 1,
            options=options
        )

    async def callback(self, interaction: discord.Interaction):
        tag_list = self.values
        filtered = [m for m in self.members_dict.values() if all(t in m.get("tags", []) for t in tag_list)]
        if not filtered:
            desc = "No members match all selected tags."
        else:
            desc = "\n".join(f"**{m['name']}** — ID `{m['id']}` — Tags: {', '.join(m.get('tags', []))}" for m in filtered)
        embed = discord.Embed(title=f"Members matching tags: {', '.join(tag_list)}", description=desc, color=discord.Color.green())
        await interaction.response.edit_message(embed=embed, view=self.view)

class TagMultiView(discord.ui.View):
    def __init__(self, available_tags, members_dict):
        super().__init__(timeout=None)
        if available_tags:
            self.add_item(TagMultiSelect(available_tags, members_dict))


# =============================================
# Co-front UI
# =============================================
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


# =============================================
# Confirmation views
# =============================================
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


# =============================================
# Multi-member removal views
# =============================================
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


# =============================================
# Slash Commands
# =============================================

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

    available_tags = get_available_tags_for_system(systems_data.get("systems", {}).get(system_id, {}))

    # Show interactive tag selector
    view = TagView(available_tags)
    await interaction.response.send_message("Select tags then press Confirm.", view=view, ephemeral=True)
    await view.wait()
    tags_list = view.selected_tags

    # Save any custom tags selected by user that are outside common presets.
    system = systems_data.get("systems", {}).get(system_id, {})
    add_custom_tags_to_system(system, tags_list)

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

    system = systems_data.get("systems", {}).get(system_id)
    if not system:
        await interaction.response.send_message("System not found.", ephemeral=True)
        return

    members = get_system_members(system_id, subsystem_id)
    if members is None:
        await interaction.response.send_message("Subsystem not found. Please check your subsystem ID.", ephemeral=True)
        return

    await interaction.response.defer(ephemeral=True)

    token_value = normalize_pluralkit_token(token)
    if not token_value:
        await interaction.followup.send("Please provide a valid PluralKit token.", ephemeral=True)
        return

    try:
        pk_system = await fetch_pluralkit_system(token_value)
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
    imported_system_tag = (pk_system.get("tag") or "").strip() or None

    if not dry_run and imported_system_tag:
        system["system_tag"] = imported_system_tag

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
    total = len(pk_members)
    BATCH_SIZE = 25  # save every 25 members to keep GitHub pushes small

    for i, raw_pk_member in enumerate(pk_members, 1):
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

        # Batch save: push to GitHub every BATCH_SIZE members to avoid huge uploads
        if not dry_run and i % BATCH_SIZE == 0:
            save_systems()
            await interaction.followup.send(
                f"Import progress: **{i}/{total}** members processed...",
                ephemeral=True
            )

    # Final save for any remaining members after the last batch
    if not dry_run:
        save_systems()

    scope_label = get_scope_label(subsystem_id)
    mode_label = "Dry Run (no changes saved)" if dry_run else "Import Complete"
    if imported_system_tag:
        system_tag_text = (
            f"Would set to **{imported_system_tag}**" if dry_run else f"Set to **{imported_system_tag}**"
        )
    else:
        system_tag_text = "No PluralKit system tag found; existing Cortex system tag left unchanged."
    await interaction.followup.send(
        f"**{mode_label}**\n"
        f"Target: {scope_label}\n"
        f"System tag: {system_tag_text}\n"
        f"PluralKit members scanned: **{total}**\n"
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
            end_front(m["id"], members_dict=members, persist=False)

    # Start fronting the selected member
    start_front(resolved_member_id, members_dict=members, persist=False)

    # Build response
    new_name = resolved_member.get("name", resolved_member_id)
    response = f"Member **{new_name}** is now fronting in {get_scope_label(subsystem_id)}."

    # Show and clear any pending inbox messages
    system = systems_data.get("systems", {}).get(system_id, {})
    cleanup_external_inbox_entries(system)
    save_systems()

    inbox = resolved_member.get("inbox", [])
    if inbox:
        external_settings = get_external_settings(system)
        delivery_mode = external_settings.get("delivery_mode", "public")

        external_msgs = [m for m in inbox if m.get("external")]
        internal_msgs = [m for m in inbox if not m.get("external")]

        response += f"\n\n\U0001f4e8 **You have {len(inbox)} message(s):**"

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
            end_front(m["id"], members_dict=members, persist=False)

    # Start the front session with co-fronts
    start_front(resolved_member_id, cofronts=selected_cofronts, members_dict=members, persist=False)
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
        lines.append(f"\u2022 **{name.title()}** \u2014 {format_duration(secs)} ({pct:.0f}%)")

    embed = discord.Embed(
        title=f"Focus Mode Stats \u2014 Last {days} Day(s)",
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
            cofront_str = f" \u2014 Co-fronting: {', '.join(cofront_names)}" if cofront_names else ""
            status_text = (current.get("status") or "").strip()
            status_str = f" \u2014 Status: {status_text}" if status_text else ""
            fronters.append(f"\u2022 {m['name']} ({scope_label}) \u2014 {duration}{status_str}{cofront_str}")

    embed = discord.Embed(
        title=title,
        description="\n".join(fronters) if fronters else "No members are currently fronting.",
        color=discord.Color.purple()
    )

    await interaction.response.send_message(embed=embed)

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
                    f"**{entry['name']}** \u2014 {start_time} \u2192 {end_time}"
                )
            else:
                lines.append(
                    f"**{entry['name']}** \u2014 {start_time} \u2192 *Currently fronting*"
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
            lines.append(f"**{name}** \u2014 {format_duration(seconds)} ({pct:.1f}%)")

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

    switch_lines = [f"\u2022 **{a} -> {b}** ({count})" for (a, b), count in top_switches]
    cofront_lines = [f"\u2022 **{a} + {b}** ({count})" for (a, b), count in top_cofronts]
    time_lines = [
        f"\u2022 **{name}**: {bucket} ({count}/{total} starts, {pct:.0f}%)"
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
        title=f"Member Stats \u2014 {member['name']}",
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
        cofront_str = f" \u2014 Co-fronts: {', '.join(cofront_names)}" if cofront_names else ""

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
    system = systems_data.get("systems", {}).get(system_id, {})
    available_tags = get_available_tags_for_system(system)
    if not available_tags:
        await interaction.response.send_message("No tags exist yet.", ephemeral=True)
        return
    embed = discord.Embed(title="Tag Browser", description="Select one or more tags from the dropdown.", color=discord.Color.blue())
    await interaction.response.send_message(embed=embed, view=TagMultiView(available_tags, members_dict))
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
        system = systems_data.get("systems", {}).get(system_id, {})
        available_tags = get_available_tags_for_system(system)
        view = TagView(available_tags, preselected=member.get("tags", []))
        await interaction.response.send_message("Select tags then press Confirm.", view=view)
        timed_out = await view.wait()
        if not timed_out:
            member["tags"] = view.selected_tags
            add_custom_tags_to_system(system, member.get("tags", []))
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
            lines.append(f"**{m['name']}** \u2014 ID `{m['id']}` \u2014 Tags: {member_tags}")

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

    system = systems_data.get("systems", {}).get(system_id)
    if not system:
        await interaction.response.send_message("System not found.", ephemeral=True)
        return

    tag = normalize_tag_value(tag)
    if not tag:
        await interaction.response.send_message("Tag cannot be blank.", ephemeral=True)
        return

    available = set(get_available_tags_for_system(system))
    if tag in available:
        await interaction.response.send_message("That tag already exists.", ephemeral=True)
        return

    add_custom_tags_to_system(system, [tag])
    save_systems()

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

    system = systems_data.get("systems", {}).get(system_id)
    if not system:
        await interaction.response.send_message("System not found.", ephemeral=True)
        return

    common_tags = sorted(set(COMMON_TAG_PRESETS))
    custom_tags = sorted(set(get_system_tag_list(system, create=False)))
    if not common_tags and not custom_tags:
        await interaction.response.send_message("No tags exist.")
        return

    sections = []
    if common_tags:
        sections.append("**Common Presets**")
        sections.extend([f"\u2022 {tag}" for tag in common_tags])
    if custom_tags:
        if sections:
            sections.append("")
        sections.append("**Your Custom Tags**")
        sections.extend([f"\u2022 {tag}" for tag in custom_tags])

    tag_lines = "\n".join(sections)

    embed = discord.Embed(
        title="System Tags",
        description=tag_lines,
        color=discord.Color.green()
    )

    await interaction.response.send_message(embed=embed)


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
                end_front(m_id, members_dict=members_dict, persist=False)
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

    system = systems_data.get("systems", {}).get(system_id)
    if not system:
        await interaction.response.send_message("System not found.", ephemeral=True)
        return

    tag = normalize_tag_value(tag)
    if not tag:
        await interaction.response.send_message("Tag cannot be blank.", ephemeral=True)
        return

    if tag in set(COMMON_TAG_PRESETS):
        await interaction.response.send_message("Common preset tags cannot be removed. Remove it from members instead.", ephemeral=True)
        return

    custom_tags = get_system_tag_list(system, create=True)
    if tag not in custom_tags:
        await interaction.response.send_message("Tag not found.", ephemeral=True)
        return

    async def do_remove(interaction: discord.Interaction):
        system["custom_tags"] = [t for t in get_system_tag_list(system, create=True) if t != tag]
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
        save_systems()
        await interaction.response.send_message(f"Tag `{tag}` removed.", ephemeral=True)

    view = ConfirmAction(confirm_callback=do_remove)
    await interaction.response.send_message(f"Are you sure you want to remove the tag `{tag}` from your system?", view=view, ephemeral=True)

# -----------------------------
# Remove member
# -----------------------------
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
# Refresh databases
# -----------------------------
@tree.command(name="refresh", description="Reload all databases from disk")
@app_commands.default_permissions(administrator=True)
async def refresh(interaction: discord.Interaction):
    global systems_data

    if os.path.exists(JSON_FILE):
        with open(JSON_FILE, "r") as f:
            systems_data = json.load(f)
    else:
        systems_data = {"systems": {}}

    total_members = sum(len(system.get("members", {})) + sum(len(sub["members"]) for sub in system.get("subsystems", {}).values()) for system in systems_data.get("systems", {}).values())
    total_custom_tags = sum(len(get_system_tag_list(system, create=True)) for system in systems_data.get("systems", {}).values())
    await interaction.response.send_message(
        f"Databases refreshed. Loaded **{len(systems_data.get('systems', {}))}** systems with **{total_members}** total members and **{total_custom_tags}** custom tags (plus common presets)."
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


def setup_slash_commands():
    """Called from cortex.py to confirm slash commands are loaded."""
    pass  # Commands are registered at import time via decorators

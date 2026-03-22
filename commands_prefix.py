@bot.command(name="pendingfriends", aliases=["pfru"])
async def pendingfriends_prefix(ctx: commands.Context):
    user_id = ctx.author.id
    system_id = get_user_system_id(user_id)
    if not system_id:
        await ctx.send("You must register using /register.")
        return
    system = systems_data["systems"].get(system_id)
    pending = get_external_settings(system).get("pending_requests", [])
    friend_reqs = [p for p in pending if p.get("type") == "friend"]
    if not friend_reqs:
        await ctx.send("No pending friend requests.")
        return
    lines = []
    for p in friend_reqs:
        sender = p.get("sender_user_id")
        sent_at = p.get("sent_at")
        lines.append(f"- <@{sender}> (ID `{sender}`) sent at {sent_at}")
    await ctx.send("Pending friend requests:\n" + "\n".join(lines))
@bot.command(name="acceptfriend", aliases=["afru"])
async def acceptfriend_prefix(ctx: commands.Context, user_id: str):
    owner_id = ctx.author.id
    system_id = get_user_system_id(owner_id)
    if not system_id:
        await ctx.send("You must register using /register.")
        return
    parsed = parse_discord_user_id(user_id)
    if not parsed:
        await ctx.send("Invalid user ID. Use a numeric Discord ID or mention.")
        return
    if str(owner_id) == parsed:
        await ctx.send("You cannot accept a friend request from yourself.")
        return
    system = systems_data["systems"].get(system_id)
    settings = get_external_settings(system)
    pending = settings.get("pending_requests", [])
    match = None
    for p in pending:
        if p.get("type") == "friend" and str(p.get("sender_user_id")) == parsed:
            match = p
            break
    if not match:
        await ctx.send("No pending friend request from that user.")
        return
    settings["pending_requests"] = [p for p in pending if p is not match]
    friends = settings.get("friend_users", [])
    if parsed not in friends:
        friends.append(parsed)
        settings["friend_users"] = friends
    sender_system_id = get_user_system_id(parsed)
    if not sender_system_id:
        await ctx.send("Sender's system not found, but your friend list was updated.")
        save_systems()
        return
    sender_system = systems_data["systems"].get(sender_system_id)
    sender_settings = get_external_settings(sender_system)
    sender_friends = sender_settings.get("friend_users", [])
    if str(owner_id) not in sender_friends:
        sender_friends.append(str(owner_id))
        sender_settings["friend_users"] = sender_friends
    save_systems()
    user = ctx.bot.get_user(int(parsed))
    if user is None:
        try:
            user = await ctx.bot.fetch_user(int(parsed))
        except Exception:
            user = None
    if user:
        try:
            await user.send(f"<@{owner_id}> has accepted your friend request! You are now mutual friends.")
        except Exception:
            pass
    await ctx.send(f"Friend request from <@{parsed}> accepted. You are now mutual friends.")

@bot.command(name="denyfriend", aliases=["dfru"])
async def denyfriend_prefix(ctx: commands.Context, user_id: str):
    owner_id = ctx.author.id
    system_id = get_user_system_id(owner_id)
    if not system_id:
        await ctx.send("You must register using /register.")
        return
    parsed = parse_discord_user_id(user_id)
    if not parsed:
        await ctx.send("Invalid user ID. Use a numeric Discord ID or mention.")
        return
    if str(owner_id) == parsed:
        await ctx.send("You cannot deny a friend request from yourself.")
        return
    system = systems_data["systems"].get(system_id)
    settings = get_external_settings(system)
    pending = settings.get("pending_requests", [])
    match = None
    for p in pending:
        if p.get("type") == "friend" and str(p.get("sender_user_id")) == parsed:
            match = p
            break
    if not match:
        await ctx.send("No pending friend request from that user.")
        return
    settings["pending_requests"] = [p for p in pending if p is not match]
    save_systems()
    user = ctx.bot.get_user(int(parsed))
    if user is None:
        try:
            user = await ctx.bot.fetch_user(int(parsed))
        except Exception:
            user = None
    if user:
        try:
            await user.send(f"<@{owner_id}> has denied your friend request.")
        except Exception:
            pass
    await ctx.send(f"Friend request from <@{parsed}> denied.")
import discord
import random
from discord import app_commands
from discord.ext import commands
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from config import (
    bot, tree, DEFAULT_FOCUS_MODES, PENDING_TIMEZONE_PROMPTS, SCHEDULED_MESSAGES,
    EXTERNAL_TARGET_LIMIT_SECONDS,
    with_instance_label,
    PROFILE_PRIVACY_LEVELS,
    TIMEZONE_FIXED_OFFSETS,
    EXTERNAL_MSG_LIMIT_COUNT,
    EXTERNAL_MSG_LIMIT_SECONDS,
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
    apply_autoproxy_mode,
    get_front_reminder_settings,
    get_birthday_reminder_settings,
    get_system_timezone_name,
    normalize_timezone_name,
    get_system_timezone,
    get_checkin_settings,
    get_checkin_trend_text,
    build_weekly_checkin_summary,
    get_focus_modes,
    get_all_mode_names,
    start_focus_mode,
    end_focus_mode,
    calc_mode_stats,
    normalize_hex,
    fit_box_drawing,
    build_member_profile_embed,
    build_subsystem_card_embed,
    resolve_member_identifier,
    resolve_member_identifier_in_system,
    start_front,
    end_front,
    format_duration,
    iter_system_member_dicts,
    get_member_sort_mode,
    sort_member_rows,
    format_member_group_lines,
    parse_discord_user_id,
    parse_bool_token,
    get_user_mode,
    is_bot_moderator_user,
    get_moderation_state,
    get_user_sanctions,
    add_mod_event,
    check_and_update_external_rate_limit,
    check_and_update_external_target_rate_limit,
    format_inbox_entry_for_channel,
    format_inbox_entry_for_dm,
    is_in_quiet_hours,
    cleanup_external_inbox_entries,
    add_external_audit_entry,
    parse_sendmessage_args,
    add_scheduled_message,
    get_effective_autoproxy_settings,
    get_server_appearance,
    get_server_member_appearance,
    resolve_target_system_for_view,
    normalize_profile_privacy_level,
    get_system_privacy_level,
    get_member_privacy_level,
    can_view_system_data,
    can_view_member_data,
    resolve_member_for_override,
    get_system_tag_list,
    normalize_tag_value,
    get_available_tags_for_system,
    add_custom_tags_to_system,
    get_group_settings,
    get_next_group_id,
    get_group_path_text,
    sorted_group_ids_for_member,
    format_playlist_link,
    normalize_embed_image_url,
    prune_external_temp_blocks,
    resolve_target_member_scope,
    get_member_from_scope,
    move_member_between_scopes,
    get_next_system_member_id,
    calculate_front_duration,
    time_of_day_bucket,
    get_fronting_member_for_user,
    find_tagged_proxy_member,
    get_member_proxy_parts,
    get_member_proxy_formats,
    render_member_proxy_format,
    render_member_proxy_result,
    add_member_proxy_format,
    remove_member_proxy_format,
    parse_proxy_format_with_placeholder,
    set_member_proxy_formats,
    normalize_proxy_formats,
    is_user_suspended,
)
from views import ConfirmAction, GroupOrderView


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
                "• Cor;systemprivacy (`spv`) <private|trusted|friends|public>\n"
                "• Cor;alterprivacy (`apv`) <member_id> <private|trusted|friends|public> [subsystem_id]\n"
                "• Cor;bulkalterprivacy (`bapv`) <member_ids> <private|trusted|friends|public> [subsystem_id]\n"
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
                "• Cor;sendmessage (`schd`) target:future time:<time> message:\"<message>\"\n"
                "  Accepts 2hrs, 1h30m, 1 day, tomorrow\n\n"
                "**Front Reminders**\n"
                "• Cor;frontreminders <true|false>\n"
                "• Cor;setfrontreminderhours <hours>\n"
                "• Cor;frontreminderstatus\n\n"
                "**Birthday Reminders**\n"
                "• Cor;birthdayreminders (`bdr`) <true|false>\n"
                "• Cor;setbirthdayreminderdays (`sbrd`) <comma_separated_days>\n"
                "• Cor;birthdayreminderstatus (`bdrs`)"
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
                "• Cor;random [public|friends|trusted|all]\n"
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
                "• Cor;frienduser (`fru`) / Cor;unfrienduser (`ufru`) / Cor;friendusers (`frus`)\n"
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
                "• Cor;refresh"
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
                "`fru` frienduser | `ufru` unfrienduser | `frus` friendusers\n"
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
        description=fit_box_drawing(profile.get("description") or "") or "No description set.",
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


# =============================================
# System Export/Import Prefix Commands
# =============================================
import io

@bot.command(name="exportsystem", aliases=["expsys"])
async def exportsystem_prefix(ctx: commands.Context):
    user_id = ctx.author.id
    system_id = get_user_system_id(user_id)
    if not system_id:
        await ctx.send("You must register a main system first using /register.")
        return
    system = systems_data["systems"].get(system_id)
    if not system:
        await ctx.send("System not found.")
        return
    export_data = deepcopy(system)
    export_data["exported_at"] = datetime.now(timezone.utc).isoformat()
    export_data["original_owner_id"] = str(user_id)
    json_bytes = json.dumps(export_data, indent=2).encode("utf-8")
    file = discord.File(fp=io.BytesIO(json_bytes), filename=f"system_{system_id}_export.json")
    try:
        await ctx.author.send(
            "Your system export is ready. Save this file and use Cor;importsystem on your new account.",
            file=file
        )
        await ctx.send("System export sent to your DMs.")
    except Exception:
        await ctx.send("Failed to send DM. Please check your DM settings.")

@bot.command(name="importsystem", aliases=["impsys"])
async def importsystem_prefix(ctx: commands.Context):
    user_id = ctx.author.id
    if get_user_system_id(user_id):
        await ctx.send("You already have a registered system. Delete it first to import.")
        return
    if not ctx.message.attachments:
        await ctx.send("Attach your exported JSON file to the command message.")
        return
    file = ctx.message.attachments[0]
    try:
        content = await file.read()
        import_data = json.loads(content.decode("utf-8"))
    except Exception:
        await ctx.send("Failed to read or parse the import file. Make sure it's a valid JSON export.")
        return
    import_data["owner_id"] = str(user_id)
    import_data.pop("original_owner_id", None)
    import_data.pop("exported_at", None)
    next_id = str(max([int(sid) for sid in systems_data["systems"].keys()] or [0]) + 1)
    systems_data["systems"][next_id] = import_data
    save_systems()
    await ctx.send(f"System imported successfully as **{import_data.get('system_name', 'Unnamed System')}**! You can now use all features.")
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


# -----------------------------
# Check-ins (prefix)
# -----------------------------
@bot.command(name="checkin", aliases=["chk"])
async def checkin_prefix(ctx: commands.Context, rating: str = None, energy: str = None, *, notes: str = None):
    user_id = ctx.author.id
    system_id = get_user_system_id(user_id)
    if not system_id:
        await ctx.send("You must register a main system first using /register.")
        return

    if rating is None or energy is None:
        await ctx.send(
            "Usage: `Cor;checkin <rating 1-10> <energy>` [notes]\n"
            "Energy options: very_low, low, medium, high, very_high"
        )
        return

    try:
        rating_value = int(rating)
    except ValueError:
        await ctx.send("Rating must be a number between 1 and 10.")
        return

    if rating_value < 1 or rating_value > 10:
        await ctx.send("Rating must be between 1 and 10.")
        return

    normalized_energy = (energy or "").strip().lower().replace("-", "_").replace(" ", "_")
    energy_aliases = {
        "verylow": "very_low",
        "vlow": "very_low",
        "vlow": "very_low",
        "very_low": "very_low",
        "low": "low",
        "med": "medium",
        "medium": "medium",
        "high": "high",
        "vhigh": "very_high",
        "veryhigh": "very_high",
        "very_high": "very_high",
    }
    normalized_energy = energy_aliases.get(normalized_energy, normalized_energy)
    valid_energies = {"very_low", "low", "medium", "high", "very_high"}
    if normalized_energy not in valid_energies:
        await ctx.send("Invalid energy level. Use one of: very_low, low, medium, high, very_high.")
        return

    system = systems_data["systems"].get(system_id)
    if not system:
        await ctx.send("System not found.")
        return

    _, front_member = get_fronting_member_for_user(user_id)
    entry = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "rating": rating_value,
        "energy": normalized_energy,
        "notes": notes,
        "front_member_id": front_member.get("id") if front_member else None,
        "front_member_name": front_member.get("name") if front_member else None,
    }

    settings = get_checkin_settings(system)
    settings.setdefault("entries", []).append(entry)
    if len(settings["entries"]) > 500:
        settings["entries"] = settings["entries"][-500:]

    save_systems()

    trend_text = get_checkin_trend_text(system)
    energy_text = normalized_energy.replace("_", " ").title()
    front_text = front_member.get("name") if front_member else "No active fronter"
    await ctx.send(
        f"Check-in logged: **{rating_value}/10**, energy **{energy_text}**.\n"
        f"Current front: **{front_text}**\n"
        f"{trend_text}"
    )


@bot.command(name="weeklymoodsummary", aliases=["wms"])
async def weeklymoodsummary_prefix(ctx: commands.Context, enabled: str = None):
    parsed = parse_bool_token(enabled)
    if parsed is None:
        await ctx.send("Usage: `Cor;weeklymoodsummary <true|false>`")
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

    settings = get_checkin_settings(system)
    settings["weekly_dm_enabled"] = parsed
    save_systems()

    status = "enabled" if parsed else "disabled"
    await ctx.send(f"Weekly mood summary DMs are now **{status}**.")


@bot.command(name="checkinstatus", aliases=["chs"])
async def checkinstatus_prefix(ctx: commands.Context):
    user_id = ctx.author.id
    system_id = get_user_system_id(user_id)
    if not system_id:
        await ctx.send("You must register a main system first using /register.")
        return

    system = systems_data["systems"].get(system_id)
    if not system:
        await ctx.send("System not found.")
        return

    settings = get_checkin_settings(system)
    trend_text = get_checkin_trend_text(system)
    weekly_status = "enabled" if settings.get("weekly_dm_enabled", True) else "disabled"
    await ctx.send(
        f"Weekly mood summaries: **{weekly_status}**\n"
        f"{trend_text}"
    )


@bot.command(name="birthdayreminders", aliases=["bdr"])
async def birthdayreminders_prefix(ctx: commands.Context, enabled: str = None):
    parsed = parse_bool_token(enabled)
    if parsed is None:
        await ctx.send("Usage: `Cor;birthdayreminders <true|false>`")
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

    settings = get_birthday_reminder_settings(system)
    settings["enabled"] = parsed
    save_systems()

    status = "enabled" if parsed else "disabled"
    day_offsets = settings.get("days_before", [2, 1])
    day_text = ", ".join(str(int(d)) for d in day_offsets)
    await ctx.send(f"Birthday reminders are now **{status}**. Day offsets: **{day_text}** day(s) before.")


@bot.command(name="setbirthdayreminderdays", aliases=["sbrd"])
async def setbirthdayreminderdays_prefix(ctx: commands.Context, *, days: str = None):
    if not days:
        await ctx.send("Usage: `Cor;setbirthdayreminderdays <comma_separated_days>` (example: `Cor;setbirthdayreminderdays 7,3,1`)")
        return

    parsed_days = []
    for part in str(days).split(","):
        token = part.strip()
        if not token:
            continue
        if not token.isdigit():
            await ctx.send("Invalid format. Use comma-separated whole numbers like `2,1` or `7,3,1`.")
            return
        value = int(token)
        if value < 0 or value > 365:
            await ctx.send("Each day offset must be between 0 and 365.")
            return
        if value not in parsed_days:
            parsed_days.append(value)

    if not parsed_days:
        await ctx.send("Provide at least one day offset, e.g. `2,1`.")
        return

    if len(parsed_days) > 12:
        await ctx.send("Please provide at most 12 day offsets.")
        return

    parsed_days.sort(reverse=True)

    user_id = ctx.author.id
    system_id = get_user_system_id(user_id)
    if not system_id:
        await ctx.send("You must register a main system first using /register.")
        return

    system = systems_data["systems"].get(system_id)
    if not system:
        await ctx.send("System not found.")
        return

    settings = get_birthday_reminder_settings(system)
    settings["days_before"] = parsed_days
    save_systems()

    day_text = ", ".join(str(day) for day in parsed_days)
    await ctx.send(f"Birthday reminder day offsets set to **{day_text}** day(s) before.")


@bot.command(name="birthdayreminderstatus", aliases=["bdrs"])
async def birthdayreminderstatus_prefix(ctx: commands.Context):
    user_id = ctx.author.id
    system_id = get_user_system_id(user_id)
    if not system_id:
        await ctx.send("You must register a main system first using /register.")
        return

    system = systems_data["systems"].get(system_id)
    if not system:
        await ctx.send("System not found.")
        return

    settings = get_birthday_reminder_settings(system)
    enabled_text = "enabled" if settings.get("enabled", True) else "disabled"
    day_offsets = settings.get("days_before", [2, 1])
    day_text = ", ".join(str(int(d)) for d in day_offsets)
    await ctx.send(f"Birthday reminders are **{enabled_text}**. Day offsets: **{day_text}** day(s) before.")


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
            end_front(m["id"], members_dict=members, persist=False)

    start_front(resolved_member_id, members_dict=members, persist=False)

    response = f"Member **{resolved_member.get('name', resolved_member_id)}** is now fronting in {get_scope_label(target_scope_id)}."
    cleanup_external_inbox_entries(system)
    save_systems()

    inbox = resolved_member.get("inbox", [])
    if inbox:
        external_settings = get_external_settings(system)
        delivery_mode = external_settings.get("delivery_mode", "public")

        external_msgs = [m for m in inbox if m.get("external")]
        internal_msgs = [m for m in inbox if not m.get("external")]

        response += f"\n\n\U0001f4e8 **You have {len(inbox)} message(s):**"

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


@bot.command(name="cofrontmember", aliases=["cfm", "cofrontmembers"])
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
            end_front(m["id"], members_dict=members, persist=False)

    start_front(resolved_member_id, cofronts=selected_cofronts, members_dict=members, persist=False)
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
    print("[DEBUG] members_prefix called")
    requester_id = ctx.author.id
    print(f"[DEBUG] requester_id: {requester_id}, target_user_id: {target_user_id}")
    system_id, system, target_owner_id, error = resolve_target_system_for_view(requester_id, target_user_id)
    print(f"[DEBUG] resolve_target_system_for_view: system_id={system_id}, error={error}")
    if error:
        await ctx.send(error)
        print(f"[DEBUG] Exiting: {error}")
        return

    # Determine viewer access level
    viewer_id = str(requester_id)
    owner_id = str(system.get("owner_id") or "")
    access_levels = {"public"}
    if viewer_id == owner_id:
        access_levels = {"public", "friends", "trusted"}
    else:
        settings = get_external_settings(system)
        trusted = set(str(uid) for uid in settings.get("trusted_users", []))
        friends = set(str(uid) for uid in settings.get("friend_users", []))
        print(f"[DEBUG] trusted={trusted}, friends={friends}, viewer_id={viewer_id}")
        if viewer_id in trusted:
            access_levels = {"public", "friends", "trusted"}
        elif viewer_id in friends:
            access_levels = {"public", "friends"}

    scope_lower = (scope or "main").strip().lower()
    def is_tracked(member):
        return not member.get("untracked", False)

    print(f"[DEBUG] scope_lower={scope_lower}")
    if scope_lower == "all":
        member_rows = []
        scoped_members_lookup = {}
        for scope_id, scoped_members in iter_system_member_dicts(system):
            scoped_members_lookup[scope_id] = scoped_members
            for member_id, member in scoped_members.items():
                privacy_level = get_member_privacy_level(member)
                if privacy_level not in access_levels:
                    continue
                if not is_tracked(member):
                    continue
                member_rows.append((scope_id, member_id, member))
        title_scope = "Entire System"
    elif scope_lower in {"main", "none"}:
        members_dict = get_system_members(system_id, None)
        print(f"[DEBUG] members_dict keys: {list(members_dict.keys()) if members_dict else 'None'}")
        scoped_members_lookup = {None: members_dict}
        member_rows = []
        for member_id, member in members_dict.items():
            privacy_level = get_member_privacy_level(member)
            if privacy_level not in access_levels:
                continue
            if not is_tracked(member):
                continue
            member_rows.append((None, member_id, member))
        title_scope = "Main System"
    else:
        members_dict = get_system_members(system_id, scope)
        print(f"[DEBUG] subsystem members_dict keys: {list(members_dict.keys()) if members_dict else 'None'}")
        if members_dict is None:
            await ctx.send("Subsystem not found.")
            print("[DEBUG] Exiting: Subsystem not found.")
            return
        scoped_members_lookup = {scope: members_dict}
        member_rows = []
        for member_id, member in members_dict.items():
            privacy_level = get_member_privacy_level(member)
            if privacy_level not in access_levels:
                continue
            if not is_tracked(member):
                continue
            member_rows.append((scope, member_id, member))
        title_scope = get_scope_label(scope).capitalize()
    print(f"[DEBUG] member_rows count: {len(member_rows)}")
@bot.command(name="toggleuntracked", aliases=["untracked", "track", "untrack"])
async def toggle_untracked_prefix(ctx: commands.Context, member_id: str = None, subsystem_id: str = None, target_user_id: str = None):
    """Toggle the 'untracked' status for a member (exclude/include in total count)."""
    if not member_id:
        await ctx.send("Usage: Cor;toggleuntracked <member_id or name> [subsystem_id] [target_user_id]")
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

    if str(target_owner_id) != str(requester_id):
        await ctx.send("You do not have permission to modify this member.")
        return

    member["untracked"] = not member.get("untracked", False)
    save_systems()
    status = "now **untracked** (excluded from total count)" if member["untracked"] else "now **tracked** (included in total count)"
    await ctx.send(f"Member **{member.get('name', resolved_member_id)}** is {status}.")

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

        total_count = len(member_rows)
        embed = discord.Embed(
            title=f"Members List - {title_scope} (Total: {total_count}) (Page {page_num}/{total_pages})",
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


@bot.command(name="random")
async def random_prefix(ctx: commands.Context, arg1: str = None, arg2: str = None):
    valid_pools = {"public", "friends", "trusted", "all"}

    selected_pool = "all"

    if arg1:
        arg1_clean = str(arg1).strip().lower()
        if arg1_clean in valid_pools:
            selected_pool = arg1_clean
        else:
            await ctx.send("Usage: Cor;random [public|friends|trusted|all]")
            return

    requester_id = ctx.author.id
    system_id = get_user_system_id(requester_id)
    if not system_id:
        await ctx.send("You must register a main system first using /register.")
        return
    system = systems_data.get("systems", {}).get(system_id)
    if not system:
        await ctx.send("System not found.")
        return

    candidates = []
    for scope_id, members_dict in iter_system_member_dicts(system):
        for member in members_dict.values():
            if selected_pool != "all" and get_member_privacy_level(member) != selected_pool:
                continue
            candidates.append((scope_id, member))

    if not candidates:
        if selected_pool == "all":
            await ctx.send("You do not have any members yet.")
        else:
            await ctx.send(f"You do not have any members in the `{selected_pool}` privacy pool.")
        return

    scope_id, member = random.choice(candidates)
    scope_label = get_scope_label(scope_id)
    pool_label = "all visible" if selected_pool == "all" else selected_pool
    await ctx.send(
        content=f"Random {pool_label} member from {scope_label}:",
        embed=build_member_profile_embed(member, system=system)
    )


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


@bot.command(name="searchmember", aliases=["srm"])
async def searchmember_prefix(ctx: commands.Context, query: str = None, subsystem_id: str = None):
    if not query:
        await ctx.send("Usage: Cor;searchmember <query> [subsystem_id]")
        return

    user_id = ctx.author.id
    system_id = get_user_system_id(user_id)
    if not system_id:
        await ctx.send("You must register using /register.")
        return

    system = systems_data.get("systems", {}).get(system_id)
    if not system:
        await ctx.send("System not found.")
        return

    scope_token = (subsystem_id or "").strip().lower()
    search_all_scopes = scope_token == "all"

    if search_all_scopes:
        member_rows = []
        for scope_id, scoped_members in iter_system_member_dicts(system):
            for member in scoped_members.values():
                member_rows.append((scope_id, member))
    else:
        members_dict = get_system_members(system_id, subsystem_id)
        if members_dict is None:
            await ctx.send("Subsystem not found.")
            return
        member_rows = [(subsystem_id, member) for member in members_dict.values()]

    query_lower = query.strip().lower()
    if not query_lower:
        await ctx.send("Provide a name or tag to search.")
        return

    results = []
    for scope_id, member in member_rows:
        name_match = query_lower in str(member.get("name", "")).lower()
        tag_match = any(query_lower in str(tag).lower() for tag in member.get("tags", []))
        if name_match or tag_match:
            results.append((scope_id, member))

    if not results:
        await ctx.send("No members found.")
        return

    if len(results) == 1:
        scope_id, member = results[0]
        fronting = "Yes" if member.get("current_front") else "No"
        duration = format_duration(calculate_front_duration(member))
        co_fronts = ", ".join(member.get("co_fronts", [])) if member.get("co_fronts") else "None"

        embed = discord.Embed(
            title=member.get("name", "Unknown"),
            description=member.get("description", "No description."),
            color=int(member.get("color", "FFFFFF"), 16)
        )

        embed.add_field(name="Currently Fronting", value=fronting, inline=True)
        embed.add_field(name="Co-fronts", value=co_fronts, inline=True)
        embed.add_field(name="Total Front Time", value=duration, inline=False)

        tags = ", ".join(member.get("tags", [])) if member.get("tags") else "None"
        embed.add_field(name="Tags", value=tags, inline=False)
        embed.add_field(name="Groups", value=format_member_group_lines(system, member), inline=False)
        if search_all_scopes:
            embed.add_field(name="Scope", value=get_scope_label(scope_id), inline=False)

        if member.get("profile_pic"):
            embed.set_thumbnail(url=member["profile_pic"])
        if member.get("banner"):
            embed.set_image(url=member.get("banner"))

        await ctx.send(embed=embed)
        return

    lines = []
    for scope_id, member in results:
        member_tags = ", ".join(member.get("tags", [])) or "None"
        scope_text = f" | Scope: {get_scope_label(scope_id)}" if search_all_scopes else ""
        lines.append(f"**{member.get('name', 'Unknown')}** - ID `{member.get('id', 'N/A')}`{scope_text} - Tags: {member_tags}")

    embed = discord.Embed(
        title=f"Search Results ({len(results)} found)",
        description="\n".join(lines),
        color=discord.Color.blue(),
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
        "birthday_reminders": {
            "enabled": True,
            "days_before": [2, 1],
            "sent_keys": {}
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
        await ctx.send("Usage: Cor;systemprivacy <private|trusted|friends|public>")
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
        await ctx.send("Invalid privacy level. Use: private, trusted, friends, or public.")
        return

    cleaned = raw_level
    system["system_privacy"] = cleaned
    save_systems()
    await ctx.send(f"System privacy set to **{cleaned}**.")


@bot.command(name="alterprivacy", aliases=["apv"])
async def alterprivacy_prefix(ctx: commands.Context, member_id: str = None, level: str = None, subsystem_id: str = None):
    if member_id is None or level is None:
        await ctx.send("Usage: Cor;alterprivacy <member_id> <private|trusted|friends|public> [subsystem_id]")
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
        await ctx.send("Invalid privacy level. Use: private, trusted, friends, or public.")
        return

    cleaned = raw_level
    member = members_dict[member_id]
    member["privacy_level"] = cleaned
    save_systems()
    await ctx.send(f"Privacy for **{member.get('name', member_id)}** set to **{cleaned}**.")


@bot.command(name="bulkalterprivacy", aliases=["bapv"])
async def bulkalterprivacy_prefix(ctx: commands.Context, member_ids: str = None, level: str = None, subsystem_id: str = None):
    if member_ids is None or level is None:
        await ctx.send("Usage: Cor;bulkalterprivacy <member_ids> <private|trusted|friends|public> [subsystem_id]\nExample: Cor;bulkalterprivacy 1,Alice,Bob public")
        return

    user_id = ctx.author.id
    system_id = get_user_system_id(user_id)
    if not system_id:
        await ctx.send("You must register a main system first using /register.")
        return

    # If subsystem_id is not provided or is blank, use main system
    effective_subsystem_id = subsystem_id if subsystem_id not in {None, "", "main", "none", "-"} else None
    members_dict = get_system_members(system_id, effective_subsystem_id)
    if members_dict is None:
        await ctx.send("Subsystem not found.")
        return

    raw_level = str(level).strip().lower()
    if raw_level not in PROFILE_PRIVACY_LEVELS:
        await ctx.send("Invalid privacy level. Use: private, trusted, friends, or public.")
        return

    # Parse member identifiers from comma or space separated input
    tokens = [t.strip() for t in re.split(r",|\s", member_ids) if t.strip()]
    if not tokens:
        await ctx.send("Please provide at least one member ID or name (comma or space separated).")
        return

    cleaned_level = raw_level
    success_count = 0
    errors = []
    
    for identifier in tokens:
        resolved_id, resolved_member, error = resolve_member_identifier(members_dict, identifier)
        if error:
            errors.append(f"`{identifier}`: {error}")
        else:
            resolved_member["privacy_level"] = cleaned_level
            success_count += 1

    save_systems()
    
    response_lines = [f"Privacy set to **{cleaned_level}** for **{success_count}** member(s)."]
    if errors:
        response_lines.append("**Errors:**\n" + "\n".join(errors))
    
    await ctx.send("\n".join(response_lines))



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

    counts = {"private": 0, "trusted": 0, "friends": 0, "public": 0}
    total_members = 0
    sample_lines = []
    for scope_id, members_dict in iter_system_member_dicts(system):
        for member_id, member in members_dict.items():
            level = get_member_privacy_level(member)
            counts[level] = counts.get(level, 0) + 1
            total_members += 1
            if len(sample_lines) < 12:
                sample_lines.append(f"\u2022 {member.get('name', member_id)} (`{member_id}`) \u2014 {level} ({get_scope_label(scope_id)})")

    lines = [
        "**Privacy Status**",
        f"System privacy: **{get_system_privacy_level(system)}**",
        (
            f"Trusted users: **{len(get_external_settings(system).get('trusted_users', []))}** | "
            f"Friends: **{len(get_external_settings(system).get('friend_users', []))}**"
        ),
        (
            f"Alter privacy totals -> private: **{counts.get('private', 0)}**, "
            f"trusted: **{counts.get('trusted', 0)}**, "
            f"friends: **{counts.get('friends', 0)}**, "
            f"public: **{counts.get('public', 0)}**, total: **{total_members}**"
        ),
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
    friend_count = len(settings.get("friend_users", []))
    pending_count = len(settings.get("pending_requests", []))
    temp_count = len(settings.get("temp_blocks", {}))
    quiet = settings.get("quiet_hours", {})
    quiet_label = f"{quiet.get('start', 23)}-{quiet.get('end', 7)}" if quiet.get("enabled") else "off"
    await ctx.send(
        f"External messages: **{enabled}**\n"
        f"Delivery mode: **{mode}**\n"
        f"Trusted-only: **{trusted_only}** (trusted: {trusted_count}, friends: {friend_count}, pending: {pending_count})\n"
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


@bot.command(name="frienduser", aliases=["fru"])
async def frienduser_prefix(ctx: commands.Context, user_id: str):
    owner_id = ctx.author.id
    system_id = get_user_system_id(owner_id)
    if not system_id:
        await ctx.send("You must register using /register.")
        return
    parsed = parse_discord_user_id(user_id)
    if not parsed:
        await ctx.send("Invalid user ID. Use a numeric Discord ID or mention.")
        return
    if str(owner_id) == parsed:
        await ctx.send("You cannot friend yourself.")
        return
    # Get both systems
    system = systems_data["systems"].get(system_id)
    target_system_id = get_user_system_id(parsed)
    if not target_system_id:
        await ctx.send("That user does not have a registered system.")
        return
    target_system = systems_data["systems"].get(target_system_id)
    settings = get_external_settings(system)
    target_settings = get_external_settings(target_system)
    if parsed in settings.get("friend_users", []) and str(owner_id) in target_settings.get("friend_users", []):
        await ctx.send("You are already mutual friends.")
        return
    # Check if a pending request already exists
    pending = target_settings.get("pending_requests", [])
    already_pending = any(
        p.get("type") == "friend" and str(p.get("sender_user_id")) == str(owner_id)
        for p in pending
    )
    if already_pending:
        await ctx.send("Friend request already pending.")
        return
    # Add pending friend request to target
    pending.append({
        "type": "friend",
        "sender_user_id": str(owner_id),
        "from_system_id": str(system_id),
        "sent_at": datetime.now(timezone.utc).isoformat(),
    })
    target_settings["pending_requests"] = pending
    save_systems()
    # Try to DM the target user
    user = ctx.bot.get_user(int(parsed))
    if user is None:
        try:
            user = await ctx.bot.fetch_user(int(parsed))
        except Exception:
            user = None
    dm_sent = False
    if user:
        try:
            await user.send(f"You have received a friend request from <@{owner_id}>. Use `Cor;acceptfriend {owner_id}` to accept or `Cor;denyfriend {owner_id}` to deny.")
            dm_sent = True
        except Exception:
            dm_sent = False
    await ctx.send(f"Friend request sent to <@{parsed}>." + (" (DM delivered)" if dm_sent else " (Could not DM user)"))


@bot.command(name="unfrienduser", aliases=["ufru"])
async def unfrienduser_prefix(ctx: commands.Context, user_id: str):
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
    friends = settings.get("friend_users", [])
    if parsed in friends:
        friends.remove(parsed)
        settings["friend_users"] = friends
        save_systems()
    await ctx.send(f"Friend user removed: `{parsed}`.")


@bot.command(name="friendusers", aliases=["frus"])
async def friendusers_prefix(ctx: commands.Context):
    user_id = ctx.author.id
    system_id = get_user_system_id(user_id)
    if not system_id:
        await ctx.send("You must register using /register.")
        return
    system = systems_data["systems"].get(system_id)
    friends = get_external_settings(system).get("friend_users", [])
    if not friends:
        await ctx.send("No friend users.")
        return
    await ctx.send("Friend users:\n" + "\n".join([f"- {u}" for u in friends]))


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
    Usage: Cor;sendmessage target:future time:"1h 30m" message:"Your message here"
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
        f"\U0001f4c5 Message scheduled to be sent to you in **{time_display}** "
        f"(at <t:{int(send_at.timestamp())}:f>).\n\n"
        f"```\n{message}\n```"
    )


def setup_prefix_commands():
    """Called from cortex.py to confirm prefix commands are loaded."""
    pass

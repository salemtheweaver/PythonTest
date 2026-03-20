import asyncio
import discord
from discord.ext import tasks
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from config import (
    bot, tree, PROXY_PREFIX, PROXY_WEBHOOK_NAME,
    PENDING_TIMEZONE_PROMPTS, PROXY_MESSAGE_AUDIT, MAX_PROXY_AUDIT_ENTRIES,
    ORIGIN_LOOKUP_EMOJIS,
    TIMEZONE_FIXED_OFFSETS, COMMON_TIMEZONES,
    with_instance_label,
)
from data import systems_data, save_systems
from helpers import (
    get_user_system_id, get_system_members, get_scope_label,
    get_effective_autoproxy_settings, get_autoproxy_settings,
    get_fronting_member_for_user, find_tagged_proxy_member,
    get_member_from_scope, get_server_appearance, get_server_member_appearance,
    get_system_profile, get_system_proxy_tag,
    normalize_timezone_name, get_system_timezone,
    get_or_create_proxy_webhook, remember_proxied_message_origin,
    send_proxy_origin_dm, normalize_embed_image_url,
    cleanup_external_inbox_entries,
)

# Track recently proxied messages so we can detect duplicates from other proxy bots.
# Maps original_author_id -> {channel_id, content, timestamp}
_recent_cortex_proxies = {}
_PROXY_DEDUP_WINDOW_SECONDS = 5
_recent_processed_source_ids = {}


def _mark_source_message_processed(message_id: int, source: str = "message") -> bool:
    """Return True if this source message/event pair was processed recently, else mark it now."""
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(seconds=_PROXY_DEDUP_WINDOW_SECONDS)

    stale_ids = [mid for mid, ts in _recent_processed_source_ids.items() if ts < cutoff]
    for mid in stale_ids:
        _recent_processed_source_ids.pop(mid, None)

    dedupe_key = (str(source), int(message_id))
    previous = _recent_processed_source_ids.get(dedupe_key)
    if previous and previous >= cutoff:
        return True

    _recent_processed_source_ids[dedupe_key] = now
    return False


async def _cleanup_duplicate_proxy_messages(channel, original_author_id, proxied_content, our_proxied_message_id):
    """After Cortex proxies, wait briefly then delete duplicate webhook messages from other proxy bots."""
    await asyncio.sleep(1.5)

    try:
        async for msg in channel.history(limit=10, after=discord.Object(id=our_proxied_message_id - 1)):
            # Skip our own proxied message
            if msg.id == our_proxied_message_id:
                continue
            # Only target webhook messages (other proxy bots use webhooks)
            if msg.webhook_id is None:
                continue
            # Check if content matches what we just proxied
            if msg.content and msg.content.strip() == proxied_content.strip():
                try:
                    await msg.delete()
                except (discord.Forbidden, discord.NotFound, discord.HTTPException):
                    pass
    except (discord.Forbidden, discord.HTTPException):
        pass


# -----------------------------
# Bot ready
# -----------------------------
@bot.event
async def on_ready():
    print(f"Logged in as {bot.user}")

    # Commands are already registered globally — only sync manually via /synccommands
    # Syncing on every restart can trigger Cloudflare rate limits (error 1015)
    print("Skipping automatic command sync (use /synccommands to sync manually)")

    from tasks import front_reminder_loop, weekly_mood_summary_loop, scheduled_messages_loop, birthday_reminder_loop
    if not front_reminder_loop.is_running():
        front_reminder_loop.start()
    if not weekly_mood_summary_loop.is_running():
        weekly_mood_summary_loop.start()
    if not scheduled_messages_loop.is_running():
        scheduled_messages_loop.start()
    if not birthday_reminder_loop.is_running():
        birthday_reminder_loop.start()

    print("Bot is ready!")


@bot.event
async def on_message(message: discord.Message):
    print(f"[DEBUG] on_message handler loaded. Message author: {message.author}, content: '{message.content}'")
    # Never proxy bot/webhook traffic.
    if message.author.bot or message.webhook_id is not None:
        return

    # Prevent accidental double-processing of the same source message.
    if _mark_source_message_processed(message.id, source="message"):
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

    # Debug logging for proxy bypass
    print(f"[DEBUG] Received message: '{message.content}' from {message.author} (ID: {message.author.id})")
    # If message starts with a backslash, do not proxy (bypass all proxy logic)
    if message.content.startswith("\\"):
        print(f"[DEBUG] Proxy bypass triggered for message: '{message.content}'")
        return await bot.process_commands(message)


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

    # Proxy logic (skip if message starts with backslash)
    if not message.content.startswith("\\"):
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
                    if latch_member_id is not None and proxy_member is None:
                        await message.channel.send(
                            f"{message.author.mention} autoproxy is set to latch, but the saved latched member could not be found. Proxy a message with a member tag first to set a new latch target."
                        )
                        await bot.process_commands(message)
                        return
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

    proxied_message = await webhook.send(**send_kwargs)
    remember_proxied_message_origin(
        proxied_message=proxied_message,
        source_message=message,
        sender_user_id=message.author.id,
        system_id=system_id,
        scope_id=scope_id_for_latch,
        member_data=proxy_member,
    )

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

    # Clean up duplicate proxy messages from other bots (e.g. PluralKit)
    if proxied_message and final_content:
        asyncio.create_task(
            _cleanup_duplicate_proxy_messages(
                message.channel, message.author.id, final_content, proxied_message.id
            )
        )

    await bot.process_commands(message)


@bot.event
async def on_raw_reaction_add(payload: discord.RawReactionActionEvent):
    # Handle X emoji for delete, and question-mark for origin lookup
    if payload.user_id == bot.user.id:
        return
    if getattr(payload.emoji, "id", None) is not None:
        return

    audit_entry = PROXY_MESSAGE_AUDIT.get(int(payload.message_id))
    if not audit_entry:
        return

    # Ensure lookup is for the same proxied message/channel pair.
    if int(audit_entry.get("proxied_channel_id", 0)) != int(payload.channel_id):
        return

    message_channel = bot.get_channel(payload.channel_id)
    if message_channel is None:
        try:
            message_channel = await bot.fetch_channel(payload.channel_id)
        except (discord.Forbidden, discord.NotFound, discord.HTTPException):
            return

    try:
        proxied_message = await message_channel.fetch_message(payload.message_id)
    except (discord.Forbidden, discord.NotFound, discord.HTTPException, AttributeError):
        return

    # Delete proxied message if original author reacts with X emoji
    if payload.emoji.name in {"❌", "✖️", "x", "X"}:
        if payload.user_id == audit_entry.get("sender_user_id"):
            try:
                await proxied_message.delete()
            except (discord.Forbidden, discord.NotFound, discord.HTTPException):
                pass
            return

    # Handle question-mark emoji for origin lookup
    if payload.emoji.name not in ORIGIN_LOOKUP_EMOJIS:
        return

    reactor_user = bot.get_user(payload.user_id)
    if reactor_user is None:
        try:
            reactor_user = await bot.fetch_user(payload.user_id)
        except (discord.NotFound, discord.Forbidden, discord.HTTPException):
            return

    try:
        await send_proxy_origin_dm(reactor_user, audit_entry)
    except (discord.Forbidden, discord.HTTPException):
        # User likely has DMs disabled; ignore silently.
        return

    try:
        await proxied_message.remove_reaction(payload.emoji, reactor_user)
    except (discord.Forbidden, discord.NotFound, discord.HTTPException, AttributeError):
        pass


@bot.event
async def on_message_edit(before: discord.Message, after: discord.Message):
    # Ignore edits that don't change content and ignore bot/webhook traffic.
    if after.author.bot or after.webhook_id is not None:
        return
    if before.content == after.content:
        return

    # Prevent accidental double-processing of the same edited source message.
    if _mark_source_message_processed(after.id, source="edit"):
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
        else:
            mode = active_autoproxy_settings.get("mode", "off")
            if mode == "front":
                scope_id_for_latch, proxy_member = get_fronting_member_for_user(after.author.id)
                proxied_text = after.content
            elif mode == "latch":
                scope_id_for_latch = active_autoproxy_settings.get("latch_scope_id")
                latch_member_id = active_autoproxy_settings.get("latch_member_id")
                proxy_member = get_member_from_scope(system, scope_id_for_latch, latch_member_id)
                if latch_member_id is not None and proxy_member is None:
                    await after.channel.send(
                        f"{after.author.mention} autoproxy is set to latch, but the saved latched member could not be found. Proxy a message with a member tag first to set a new latch target."
                    )
                    return
                proxied_text = after.content

    if not proxy_member:
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

    proxied_message = await webhook.send(**send_kwargs)
    remember_proxied_message_origin(
        proxied_message=proxied_message,
        source_message=after,
        sender_user_id=after.author.id,
        system_id=system_id,
        scope_id=scope_id_for_latch,
        member_data=proxy_member,
    )

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

    # Clean up duplicate proxy messages from other bots (e.g. PluralKit)
    if proxied_message and final_content:
        asyncio.create_task(
            _cleanup_duplicate_proxy_messages(
                after.channel, after.author.id, final_content, proxied_message.id
            )
        )


def setup_events():
    """Called from cortex.py to confirm events are loaded."""
    pass

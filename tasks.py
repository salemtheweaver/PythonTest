# tasks.py — Background task loops

import discord
from discord.ext import tasks
from datetime import datetime, timezone, timedelta

from config import bot, SCHEDULED_MESSAGES
from data import systems_data, save_systems
from helpers import (
    get_front_reminder_settings, get_scope_label,
    iter_system_member_dicts, format_duration,
    get_checkin_settings, get_system_timezone,
    current_week_key, build_weekly_checkin_summary,
    cleanup_external_inbox_entries,
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


def setup_tasks():
    """Called from cortex.py to confirm tasks are loaded."""
    pass

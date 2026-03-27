
# Cortex Discord Bot

**Version**: 2.3.0 | **Last Updated**: March 27, 2026 | **Python**: 3.12.10 | **Deployed on**: Railway


## System Hierarchy Overview

**Cortex** supports a flexible three-level hierarchy for organizing members:

```
Main System
├── Side System: "alpha"
│   ├── Subsystem: "beta"
│   │   └── Members: Alice, Bob
│   └── Members: Charlie
├── Side System: "omega"
│   └── Members: Delta
└── Members: Echo, Foxtrot
```

- **Main System**: The root system for a user. Contains members and any number of side systems.
- **Side System**: An optional grouping under the main system. Each side system can have its own members and subsystems.
- **Subsystem**: Nested under a side system (or directly under the main system if no side system is used). Subsystems contain members.

**Command arguments:**
- Most member/group/tag commands now accept `side_id` and/or `subsystem_id` to specify the target scope.
- If only `subsystem_id` is provided, the main system's subsystems are used. If both `side_id` and `subsystem_id` are provided, the subsystem is resolved within the given side system.
- If neither is provided, the main system's members are used.

**Example usage:**
- To add a member to a side system: `/addmember side_id=alpha`
- To add a member to a subsystem within a side system: `/addmember side_id=alpha subsystem_id=beta`
- To add a member to a main system subsystem: `/addmember subsystem_id=main_sub`

**Tip:** When referencing "subsystem" in commands or documentation, it may refer to either a main system subsystem or a subsystem within a side system. Always specify `side_id` if you mean a side system's subsystem.

See below for more details on how scopes work.
## Common Patterns & Edge Cases

- Moving members: You can move members between main, side, and subsystem scopes using `/movemember` with the appropriate `side_id` and/or `subsystem_id`.
- Deleting a side system or subsystem will remove all members within that scope.
- Privacy and permissions are enforced per member, regardless of scope.
- Most listing and search commands will show results from all scopes unless filtered.

**Edge cases:**
- If a member exists in both a side system and a main system with the same name, use their unique ID for disambiguation.
- If you provide only `subsystem_id`, the main system's subsystems are used by default.
- If you provide both `side_id` and `subsystem_id`, the subsystem is resolved within the given side system.

## Project Overview

Cortex is a Discord bot for managing **plural systems** (multiple distinct identities within one user). It supports system/singlet modes, member management, message proxying via webhooks, fronting tracking, external messaging, wellness features, and moderation.

## Architecture

9-file modular structure (~13,700 lines total):

```
cortex.py          (30 lines)   Entry point — imports modules, patches interaction check, runs bot
config.py         (158 lines)   Constants, env vars, bot instance, runtime state
data.py           (324 lines)   Data persistence — local JSON + GitHub sync with background workers
helpers.py       (2612 lines)   80+ utility functions (core logic lives here)
commands_slash.py (5211 lines)   90+ slash commands
commands_prefix.py(3950 lines)   80+ prefix commands (mirrors slash commands with Cor; prefix)
events.py         (656 lines)   on_ready, on_message (proxy routing), on_message_edit, on_raw_reaction_add
tasks.py          (343 lines)   4 background loops (front reminders, mood summaries, scheduled messages, birthdays)
views.py          (443 lines)   Discord UI components (buttons, selects, modals, paginators)
```

### Module Dependencies

All modules import from `config.py` and `data.py`. `helpers.py` is the shared logic layer used by commands, events, tasks, and views. `cortex.py` wires everything together.

## Tech Stack & Dependencies

- `discord.py >= 2.3.0` — Bot framework
- `python-dotenv == 1.2.2` — Environment variable loading
- `tzdata >= 2024.1` — Timezone data
- No database — JSON files persisted to GitHub API

## Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `DISCORD_BOT_TOKEN` | Yes | Discord bot token |
| `CORTEX_ADMIN_USER_ID` | No | Single admin user ID |
| `CORTEX_ADMIN_USER_IDS` | No | Comma/semicolon-separated admin IDs |
| `GITHUB_TOKEN` | No | GitHub API token for data persistence |
| `GITHUB_REPO` | No | GitHub repo (default: `salemtheweaver/PythonTest`) |
| `CORTEX_INSTANCE_LABEL` | No | Label for multi-instance deployments |

## Data Persistence

- **Per-system files**: `systems/{system_id}.json` on GitHub
- **Monolith file**: `cortex_members.json` on GitHub (moderation state)
- **Local disk**: Session cache, written first before GitHub sync
- **Background save worker**: Non-blocking async queue with retry (6 attempts, exponential backoff capped at 60s)
- **SIGTERM handling**: `flush_pending_save()` on shutdown

## Core Concepts


### System Scopes

**Main System:**
- Members are stored at `system["members"]`.

**Side Systems:**
- Each side system is stored at `system["side_systems"][side_id]`.
- Side system members: `system["side_systems"][side_id]["members"]`
- Side system subsystems: `system["side_systems"][side_id]["subsystems"][subsystem_id]["members"]`

**Main System Subsystems:**
- Subsystems directly under the main system are at `system["subsystems"][subsystem_id]["members"]`

**Function usage:**
- To get main system members: `get_system_members(system_id)`
- To get side system members: `get_system_members(system_id, side_id=...)`
- To get subsystem members: `get_system_members(system_id, side_id=..., subsystem_id=...)`
- If only `subsystem_id` is provided, the main system's subsystems are used.

**Example data structure:**
```json
{
  "members": { ... },
  "side_systems": {
    "alpha": {
      "members": { ... },
      "subsystems": {
        "beta": {
          "members": { ... }
        }
      }
    }
  },
  "subsystems": {
    "main_sub": {
      "members": { ... }
    }
  }
}
```

### Member Structure (actual)
```json
{
  "id": "numeric_string",
  "name": "string",
  "display_name": "string|null",
  "pronouns": "string|null",
  "description": "string|null",
  "color": "hex (default: 00DE9B)",
  "profile_pic": "url|null",
  "banner": "url|null",
  "tags": ["tag1", "tag2"],
  "birthday": "YYYY-MM-DD|MM-DD|null",
  "privacy_level": "private|trusted|friends|public",
  "current_front": { "start": "ISO8601", "cofronts": [], "reminder_sent": false } | null,
  "proxy_formats": ["text [name] text"],
  "groups": ["group_id"]
}
```

### Privacy Model (4-tier)
| Level | Owner | Trusted | Friends | Public |
|-------|-------|---------|---------|--------|
| `private` | Yes | No | No | No |
| `trusted` | Yes | Yes | No | No |
| `friends` | Yes | Yes | Yes | No |
| `public` | Yes | Yes | Yes | Yes |

### Proxy System
- **Proxy tags**: Patterns like `[name]` or `name:` that trigger auto-proxying
- **Explicit prefix**: `;;` sends as fronting/latched member without tag
- **Webhook proxying**: Messages sent via Discord webhook as the member
- **Autoproxy modes**: `off`, `front` (current fronter), `latch` (last proxied)
- **Server overrides**: Per-guild autoproxy settings that override system default
- **Backslash escape**: Messages starting with `\` skip proxy entirely

### Command Gating (helpers.py)
All commands pass through `_global_interaction_check` / `prefix_command_gate`:
1. **Moderation check**: `MOD_COMMANDS` require admin status
2. **Ban/suspension check**: Banned users blocked (except `modappeal`)
3. **Singlet gate**: Singlet users restricted to `SINGLET_ALLOWED_COMMANDS`
4. **Register gate**: All commands except `/register` require registration

## Key Constants (config.py)

- **Command prefix**: `Cor;` or `cor;` (also mention-based)
- **Proxy prefix**: `;;`
- **Proxy webhook name**: `"Cortex Proxy"`
- **Rate limits**: 5 msgs/60s per user, 20s between msgs to same target
- **Max proxy audit**: 5000 entries
- **Max external audit**: 200 entries


## Command Categories (90+ slash, 80+ prefix)

**Note:** Most member/group/tag commands now accept `side_id` and/or `subsystem_id` to specify the target scope. If only `subsystem_id` is provided, the main system's subsystems are used. If both are provided, the subsystem is resolved within the given side system. If neither is provided, the main system's members are used.

**Examples:**
- `/addmember side_id=alpha` — Add a member to a side system
- `/addmember side_id=alpha subsystem_id=beta` — Add a member to a subsystem within a side system
- `/addmember subsystem_id=main_sub` — Add a member to a main system subsystem

### System Setup
`/register`, `/createsubsystem`, `/editsubsystem`, `/deletesystem`, `/clearsystem`, `/clearall`, `/refresh`, `/synccommands`

### System Profile
`/viewsystemcard`, `/editsystemcard`, `/viewsubsystemcard`, `/editsubsystemcard`, `/listsubsystems`, `/systemtag`, `/systemprivacy`

### Member Management
`/addmember`, `/members`, `/viewmember`, `/editmember`, `/editmemberimages`, `/removemember`, `/removemembers`, `/searchmember`, `/random`, `/membersort`, `/movemember`

> **Note:** For all member/group/tag commands, `side_id` and/or `subsystem_id` can be used to specify the target scope. If only `subsystem_id` is provided, the main system's subsystems are used. If both are provided, the subsystem is resolved within the given side system. If neither is provided, the main system's members are used. When referencing "subsystem" in commands or documentation, it may refer to either a main system subsystem or a subsystem within a side system. Always specify `side_id` if you mean a side system's subsystem.
### Development Guidelines

> **Subsystem Reference:** Throughout the codebase and documentation, "subsystem" may refer to either a main system subsystem or a subsystem within a side system. Always clarify which is meant, and use both `side_id` and `subsystem_id` in function signatures and command arguments when targeting a side system's subsystem.

### Proxy & Tags
`/editmembertag`, `/addmembertag`, `/removemembertag`, `/addtag`, `/removetag`, `/listtags`, `/browsetags`

### Fronting & Switching
`/switchmember`, `/cofrontmember`, `/currentfronts`, `/clearfront`, `/fronthistory`, `/frontstats`, `/switchpatterns`, `/memberstats`, `/memberhistory`

### Autoproxy
`/globalautoproxy`, `/autoproxy`, `/autoproxystatus`

### Groups
`/creategroup`, `/editgroup`, `/deletegroup`, `/listgroups`, `/grouporder`, `/grouporderui`, `/addmembergroup`, `/removemembergroup`, `/membergroups`

### Privacy & Safety
`/privacystatus`, `/alterprivacy`, `/bulkalterprivacy`, `/blockuser`, `/unblockuser`, `/blockedusers`, `/trustuser`, `/untrustuser`, `/trustedusers`, `/frienduser`, `/unfrienduser`, `/friendusers`, `/muteuser`, `/unmuteuser`, `/mutedusers`, `/tempblockuser`, `/tempblockedusers`

### External Messaging
`/allowexternal`, `/externalstatus`, `/externalprivacy`, `/externallimits`, `/externaltrustedonly`, `/externalquiethours`, `/externalretention`, `/externalpending`, `/approveexternal`, `/recentexternal`, `/sendexternal`, `/reportexternal`

### Server Identity
`/serveridentity`, `/serveridentitystatus`, `/servermemberidentity`, `/servermemberidentitystatus`

### Timezone & Focus
`/settimezone`, `/timezonestatus`, `/setmode`, `/currentmode`, `/modestats`

### Wellness
`/checkin`, `/checkinstatus`, `/weeklymoodsummary`

### Reminders
`/frontreminders`, `/setfrontreminderhours`, `/frontreminderstatus`, `/birthdayreminders`, `/setbirthdayreminderdays`, `/birthdayreminderstatus`

### Scheduled Messages (singlet)
`/sendmessage`

### Moderation (admin only)
`/modreports`, `/modwarn`, `/modsuspend`, `/modban`, `/modunban`, `/modappeal`


### Import/Export
`/exportsystem`, `/importsystem`, `/importpluralkit`

> **Note:** Import/export commands and logic must support the full hierarchy, including side systems and subsystems. When importing, ensure members are placed in the correct scope (main, side, or subsystem) based on provided arguments. When exporting, preserve the hierarchy in the output data.


## Background Tasks (tasks.py)

> **Note:** All background tasks (reminders, mood summaries, scheduled messages, birthdays) should iterate over all members in all scopes (main, side, and subsystems). Use hierarchy-aware helpers to avoid missing members in side systems or subsystems.

| Task | Interval | Purpose |
|------|----------|---------|
| `front_reminder_loop` | 5 min | DM owner if member fronting > configured hours |
| `weekly_mood_summary_loop` | 24 hours | Send weekly check-in summary DM |
| `scheduled_messages_loop` | 1 min | Deliver queued scheduled messages |
| `birthday_reminder_loop` | 6 hours | DM birthday reminders N days before |


## Event Handlers (events.py)

> **Note:** All event handlers (proxying, switching, reactions) must use hierarchy-aware helpers and pass scope information (side_id, subsystem_id) as needed. This ensures correct behavior for all member operations regardless of scope.

- **on_ready**: Logs login, starts background tasks (no auto command sync)
- **on_message**: Timezone prompt handling, proxy routing (explicit `;;`, tagged, autoproxy), webhook send, reply embeds, duplicate cleanup
- **on_message_edit**: Same proxy logic as on_message
- **on_raw_reaction_add**: X emoji = delete proxied msg (by original author), ? emoji = DM about message origin

## Discord UI Views (views.py)

- **GroupOrderView**: Interactive group reorder with focus/move buttons (3 min timeout)
- **TagView/TagSelect/ConfirmTags**: Multi-select tag dropdown
- **TagMultiView/TagMultiSelect**: Filter members by tag intersection
- **CoFrontView/CoFrontSelect**: Paginated co-front member selection
- **ConfirmAction**: Generic confirm/cancel dialog
- **ConfirmRemove**: Member removal confirmation
- **MultiMemberView/MultiMemberSelect**: Paginated bulk member select
- **ConfirmClearSystem**: Dangerous clear-all confirmation
- **HelpPaginator** (in commands_prefix.py): 10-page help menu with jump dropdown

## Version History


| Version | Date | Summary |
|---------|------|---------|
| **2.3.0** | March 27, 2026 | Major update: Side system ID assignment logic unified across all commands and imports; fixed all syntax errors; improved import robustness; version bump. |
| **2.2.1** | March 20, 2026 | Member card layout fixes, random scope controls, PK import improvements |
| **2.2.0** | March 19, 2026 | Multi-admin support, 4-tier privacy model, friend lists, ? reaction member cards |
| **2.1.3** | March 19, 2026 | Proxy deduplication, register UX (optional system name) |
| **2.1.2** | March 19, 2026 | PluralKit token normalization, import diagnostics |
| **2.1.1** | March 19, 2026 | Background save worker (non-blocking), register defer fix |
| **2.1.0** | March 19, 2026 | Batched PK import, GitHub retry logic, compact JSON, duplicate proxy cleanup |
| **2.0.0** | March 19, 2026 | Major refactor: monolithic (~11K lines) to 9-file modular architecture |
| **1.9.0** | Pre-refactor | Monolithic single-file version (deprecated) |

### Changelog

#### [2.2.1] — March 20, 2026 — Member Card Layout & Random Scope Controls
- `/random` and `Cor;random` now support privacy-pool filters (`public`, `friends`, `trusted`, `all`)
- Random commands restricted to caller's own system
- Member card formatting preserves PluralKit-style description line breaks
- Metadata moved into dedicated embed fields to avoid disrupting card layout
- Moderation/admin state now persists to GitHub alongside per-system saves
- `/refresh` updates shared in-memory data object in place so modules stay in sync

#### [2.2.0] — March 19, 2026 — Multi-Admin, Privacy Tiers & Stability
- Multi-admin support via `CORTEX_ADMIN_USER_IDS` env var
- Persisted bot-admin user IDs in `_moderation.bot_admin_user_ids`
- Four-level privacy model: `private`, `trusted`, `friends`, `public`
- Friend list management commands (slash and prefix)
- Member card embed on `?` reaction for accessible members
- Friendly time parsing for scheduled messages (`30mins`, `1h30m`, `tomorrow`)
- Restored missing prefix check-in commands
- Fixed `SyntaxWarning` in privacystatus, `Interaction already acknowledged` in synccommands
- GitHub 409 conflict retries: 6 attempts with exponential backoff (capped at 60s)

#### [2.1.3] — March 19, 2026 — Proxy Stability & Register UX
- Source-message deduping in `on_message` and `on_message_edit`
- `/register` falls back to user's display name when system_name omitted

#### [2.1.2] — March 19, 2026 — PluralKit Import Diagnostics
- PK token normalization strips angle-brackets and hidden characters
- `/importpluralkit` shows API response details and per-member error samples

#### [2.1.1] — March 19, 2026 — Interaction Timeout Fixes
- `save_systems()` now writes locally first and queues GitHub sync on background worker
- `/register` defers interaction before saving to avoid timeout

#### [2.1.0] — March 19, 2026 — Reliability & Import Improvements
- Batched PK import (saves every 25 members with progress updates)
- GitHub save retry logic with exponential backoff
- Compact JSON encoding, scaled timeouts, size warnings
- Duplicate proxy cleanup for other bots (e.g., PluralKit)
- Fixed autoproxy settings not persisting across restarts

#### [2.0.0] — March 19, 2026 — Monolithic to Modular Architecture
- Refactored single `cortex.py` (~11,000 lines) into 9 focused modules
- Added privacy tier system, friend list management
- Centralized configuration, separated concerns

#### [1.9.0] — Pre-Refactor (Deprecated)
- Original monolithic single-file bot with all core features

### Post-2.2.1 Fixes (unreleased)
- Fixed front duration double-counting, shutdown race condition, scheduled message persistence
- Fixed `/members` paginator state and page navigation
- Added untracked member support (`/toggleuntracked`) — excluded from member listings
- Birthday reminder `sent_keys` now always persisted after reminders run
- Member card layout iterations (box-drawing, embed fields, mobile formatting)
- Member list shows public members without system-level gate
- `help` command now bypasses registration and singlet gate checks
- Added `railway.toml` with watch patterns so `systems/*.json` data saves don't trigger Railway redeployments

### Versioning Convention
- **MAJOR**: Breaking changes to core functionality
- **MINOR**: New features, backwards compatible
- **PATCH**: Bug fixes, documentation updates


## Help Output & Usage Messages

All help commands, paginators, and usage messages should:
- Clearly mention the Main > Side System > Subsystem hierarchy.
- Show example usage with `side_id` and/or `subsystem_id` where relevant.
- Clarify in help text when a command targets a main system, a side system, or a subsystem within a side system.
- Use consistent terminology: "side system" for the intermediate grouping, "subsystem" for the lowest level (nested under either main or side).

**Example help text:**
> `/addmember side_id=alpha subsystem_id=beta` — Adds a member to subsystem "beta" within side system "alpha".
> `/members subsystem_id=main_sub` — Lists members in a main system subsystem.
> If neither argument is provided, the main system's members are used.

Update all help output and command descriptions to reflect these patterns for user clarity.

## Developer Notes

- Always use hierarchy-aware helpers (e.g., `get_system_members`, `get_side_system`, `get_subsystem`) in commands, UI, and event handlers.
- When adding new features, ensure all member/group/tag operations accept and pass `side_id` and/or `subsystem_id` as needed.
- When referencing "subsystem" in code or docs, clarify if it is a main system subsystem or a side system's subsystem.
- When iterating over all members, use helpers that traverse all scopes (main, side, and subsystems).
- When in doubt, prefer explicit argument names and docstrings to avoid ambiguity.


### Adding a Command
1. Add slash command in `commands_slash.py` using `@tree.command()`
2. Mirror as prefix command in `commands_prefix.py` using `@bot.command()`
3. Add to `SINGLET_ALLOWED_COMMANDS` or `MOD_COMMANDS` in config.py if needed
4. Use `await interaction.response.defer()` for operations that may take >3 seconds
5. For member/group/tag commands, ensure you accept and pass `side_id` and/or `subsystem_id` to helpers for correct scope resolution.

### Saving Data
Always call `save_systems()` after modifying `systems_data` — it writes locally first, then queues GitHub sync in background.


### Key Patterns
- System lookup: `get_user_system_id(user_id)` → system_id
- Member resolve: `resolve_member_identifier_in_system(system, token, subsystem_id, side_id=None)` (always clarify which subsystem)
- Privacy check: `can_view_member_data(member, requester_id)`
- Fronting: `start_front()` / `end_front()` / `get_fronting_member_for_user()`
- Always use helpers that accept both `side_id` and `subsystem_id` for member/group/tag operations.
- Prefer explicit argument names and docstrings to clarify scope.


## File Listing

> **Note:** When reading or editing code, always clarify whether a "subsystem" reference is for a main system subsystem or a side system's subsystem. File and function names should be explicit when possible.

```
d:\Programming\Discord bot\
├── cortex.py                  Entry point
├── config.py                  Configuration
├── data.py                    Data persistence
├── helpers.py                 Utility functions
├── commands_slash.py          Slash commands
├── commands_prefix.py         Prefix commands
├── events.py                  Event handlers
├── tasks.py                   Background tasks
├── views.py                   UI components
├── requirements.txt           Dependencies
├── runtime.txt                Python version (3.12.10)
├── Procfile                   Railway deployment
├── railway.toml               Railway config (watch patterns to ignore data file commits)
├── .gitignore                 Ignores .env, __pycache__, cortex_members.json
├── cortex_members.json        Local data cache (GitHub synced)
├── tags.json                  Shared tag system
├── CLAUDE.md                  This file (auto-loaded by Claude Code)
└── systems/                   Per-system JSON data files
    └── 1.json through 9.json

  ## Testing & Validation

  - Update or add tests to cover all hierarchy scenarios: main, side, and subsystem operations for all commands and UI components.
  - Validate that all UI components (views, paginators, selectors) work with side systems and subsystems.
  - Test edge cases: moving members between scopes, deleting sides/subsystems, privacy/permission checks across all scopes.
```

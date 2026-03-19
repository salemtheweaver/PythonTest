# Cortex Discord Bot — Codebase Documentation

**Version**: 2.0.0 (Modular Architecture)  
**Last Updated**: March 19, 2026

## Table of Contents
1. [Version Control](#version-control)
2. [Project Overview](#project-overview)
3. [Architecture](#architecture)
4. [Module Breakdown](#module-breakdown)
5. [Core Concepts](#core-concepts)
6. [Data Structures](#data-structures)
7. [Key Helper Functions](#key-helper-functions)
8. [Command Structure](#command-structure)
9. [Configuration and Environment](#configuration-and-environment)
10. [Privacy Model](#privacy-model)
11. [External Messaging System](#external-messaging-system)

---

## Version Control

### Current Version
- **Version**: 2.1.2
- **Release Date**: March 19, 2026
- **Status**: Stable
- **Architecture**: Modular (9-file structure)

### Changelog

#### [2.1.2] — March 19, 2026
**PluralKit Import Diagnostics**

**Changes**:
- PluralKit token normalization now strips angle-bracket wrappers and embedded whitespace/control characters from pasted tokens
- `/importpluralkit` now includes API response details for rejected tokens and rate-limit responses
- Import summaries now include sample per-member errors instead of silently counting failures

**Bug Fixes**:
- Reduced cases where valid pasted PluralKit tokens fail because of hidden formatting characters
- Made partial import failures diagnosable from the command output

**Breaking Changes**: None

#### [2.1.1] — March 19, 2026
**Interaction Timeout Fixes**

**Changes**:
- `save_systems()` now writes locally first and queues GitHub sync on a background worker instead of blocking command execution
- Slash command `/register` now defers its interaction before saving and replies with a follow-up message

**Bug Fixes**:
- Fixed `Unknown interaction` failures when `/register` hit Discord's interaction timeout during GitHub-backed saves

**Breaking Changes**: None

#### [2.1.0] — March 19, 2026
**Reliability & Import Improvements**

**Changes**:
- **Batched PluralKit import**: Large imports now save every 25 members with progress updates, preventing GitHub upload timeouts
- **GitHub save retry logic**: Saves now retry up to 3 times with exponential backoff on failure
- **Compact JSON encoding**: GitHub-stored JSON uses minimal whitespace to reduce file size
- **Scaled timeouts**: GitHub upload timeout now scales with file size (30s base + 5s per MB)
- **Size warnings**: Logs a warning when `cortex_members.json` approaches GitHub's 100MB limit
- **Duplicate proxy cleanup**: Already-proxied messages from other bots (e.g. PluralKit) are auto-deleted to prevent duplicates

**Bug Fixes**:
- Autoproxy settings now reliably persist across restarts (GitHub save failures were silently dropping data)

#### [2.0.0] — March 19, 2026
**Major Refactor: Monolithic to Modular Architecture**

**Changes**:
- Refactored single `cortex.py` (~11,000 lines) into 9 focused modules
- **New module structure**:
  - `cortex.py` — Entry point only
  - `config.py` — Constants and configuration
  - `data.py` — Data persistence and GitHub sync
  - `helpers.py` — Utility functions (~2,000+ lines)
  - `commands_slash.py` — Slash command handlers
  - `commands_prefix.py` — Prefix command handlers
  - `events.py` — Discord event handlers
  - `tasks.py` — Background task loops
  - `views.py` — Discord UI components

**Features Added**:
- Privacy tier system (private, friends, trusted, public)
- Friend list management (frienduser, unfrienduser, friendusers)
- Modulation architecture for better maintainability
- Centralized configuration management
- Separated concerns: commands, data, helpers, events

**Breaking Changes**: None (internal refactor only)

**Dependencies**:
- discord.py==2.7.1
- python-dotenv==1.1.0
- pytz==2025.2

---

#### [1.9.0] — Pre-Refactor Version
**Note**: This version existed as a single monolithic `cortex.py` file.

**Features**:
- System/subsystem/member management
- Proxying with webhooks
- Fronting tracking
- External messaging with privacy controls
- Autoproxy
- Wellness check-ins and mood tracking
- Focus modes (singlet feature)
- Moderation system
- Group management
- Tag system

---

### Version History Quick Reference

| Version | Date | Architecture | Status |
|---------|------|--------------|--------|
| 2.0.0 | March 19, 2026 | Modular (9 files) | Current/Stable |
| 1.9.0 | Pre-refactor | Monolithic (1 file) | Deprecated |

### How to Track Changes

**When making any code change**:
1. Update the relevant section in this documentation
2. If significant, add entry to changelog with:
   - Date
   - Version number (use semantic versioning: MAJOR.MINOR.PATCH)
   - Summary of changes
   - Breaking changes (if any)
   - New/modified features

**Semantic Versioning**:
- **MAJOR**: Breaking changes to API/core functionality
- **MINOR**: New features, backwards compatible
- **PATCH**: Bug fixes, documentation updates

**Example changelog entry**:
```
#### [2.1.0] — March 20, 2026
**Features**:
- Added `/sendevent` command for logging system events
- Improved member search performance

**Bug Fixes**:
- Fixed timezone conversion for DST transitions

**Dependencies**:
- Updated discord.py to 2.7.2
```

---

## Project Overview

**Cortex** is a Discord bot designed to help manage plural systems (multiple distinct identities within one user). It provides tools for:

- **System Management**: Register systems, manage members and subsystems
- **Proxying**: Send messages as different members using proxy tags
- **Fronting Tracking**: Track which member is currently "fronting" (in control)
- **External Messaging**: Enable intra-system DMs with privacy controls
- **Wellness Features**: Check-in reminders, mood tracking, weekly summaries
- **Focus Modes**: Track time spent in different modes (studying, gaming, etc.)
- **Moderation**: User bans, warnings, suspensions
- **Autoproxy**: Automatically determine which member should proxy

**Tech Stack**:
- Python 3.x
- Discord.py 2.7.1
- Pytz for timezone management
- GitHub API for data persistence
- Async/await for non-blocking operations

---

## Architecture

The codebase has been refactored into a **modular architecture** with 9 focused files:

```
cortex.py                 Entry point, bot initialization
├── config.py            Constants, environment variables
├── data.py              Data persistence, GitHub sync
├── helpers.py           Utility functions (~2000+ lines)
├── commands_slash.py    Slash command handlers
├── commands_prefix.py   Text prefix command handlers
├── events.py            Discord event handlers
├── tasks.py             Background task loops
├── views.py             Discord UI components (buttons, selects)
└── requirements.txt     Python dependencies
```

### Module Relationships

```
cortex.py (entry point)
  │
  ├─→ config.py (constants and bot instance)
  ├─→ data.py (load/save JSON data)
  ├─→ helpers.py (utility functions)
  │   └─→ config.py, data.py
  ├─→ commands_slash.py (slash commands)
  │   └─→ config.py, data.py, helpers.py
  ├─→ commands_prefix.py (prefix commands)
  │   └─→ config.py, data.py, helpers.py
  ├─→ events.py (event handlers)
  │   └─→ config.py, data.py, helpers.py
  ├─→ tasks.py (background loops)
  │   └─→ config.py, data.py, helpers.py
  └─→ views.py (UI components)
      └─→ config.py, data.py, helpers.py
```

---

## Module Breakdown

### 1. **cortex.py** — Entry Point

**Purpose**: Wires all modules together and starts the bot.

**Key Responsibilities**:
- Import all modules in correct order
- Patch the global interaction check before command registration
- Execute `bot.run(TOKEN)`

**Code Structure**:
```python
# Load configuration and bot instance
from config import bot, TOKEN, CortexCommandTree

# Patch interaction check with helpers
from helpers import _global_interaction_check
CortexCommandTree.interaction_check = lambda self, interaction: _global_interaction_check(interaction)

# Register commands, events, tasks
import commands_slash
import commands_prefix
import events
import tasks
from helpers import prefix_command_gate

# Start bot
bot.run(TOKEN)
```

---

### 2. **config.py** — Configuration & Constants

**Purpose**: Centralized configuration, environment variables, and bot instance.

**Key Exports**:

#### Environment Variables
```python
TOKEN                   # Discord bot token (from .env)
ADMIN_USER_ID          # Admin user ID
GITHUB_TOKEN           # GitHub API token for data persistence
GITHUB_REPO            # GitHub repo for backups (default: salemtheweaver/PythonTest)
INSTANCE_LABEL         # Optional label for bot instance
```

#### File Paths
```python
JSON_FILE              # "cortex_members.json" — main data file
TAGS_FILE              # "tags.json" — tag definitions
```

#### Proxy & Rate Limiting
```python
PROXY_PREFIX = ";;"    # Message prefix trigger (e.g., ";;message")
PROXY_WEBHOOK_NAME     # Webhook name for proxied messages
EXTERNAL_MSG_LIMIT_COUNT = 5           # Max 5 messages
EXTERNAL_MSG_LIMIT_SECONDS = 60        # Per 60 seconds
EXTERNAL_TARGET_LIMIT_SECONDS = 20     # Target-specific rate limit
EXTERNAL_AUDIT_MAX = 200               # Max audit entries
MAX_PROXY_AUDIT_ENTRIES = 5000         # Max proxy audits
ORIGIN_LOOKUP_EMOJIS = {"❓", "❔"}     # React for origin lookup
```

#### Command Controls
```python
ALLOWED_WHEN_BOT_BANNED = {"modappeal"}
SINGLET_ALLOWED_COMMANDS = {                # Commands available in singlet mode
    "register", "allowexternal", "sendexternal",
    "checkin", "settimezone", "refresh", ...
}
MOD_COMMANDS = {"modreports", "modwarn", ...}
PROFILE_PRIVACY_LEVELS = {"private", "trusted", "public"}
```

#### Timezone & Preset Data
```python
DEFAULT_FOCUS_MODES = ["studying", "gaming", "social", "burnout", "rest", ...]
COMMON_TAG_PRESETS = ["host", "co-host", "protector", "fictive", ...]
TIMEZONE_ALIASES = {"EST": "America/New_York", ...}
TIMEZONE_FIXED_OFFSETS = {"UTC": 0, "America/New_York": -5, ...}
```

#### Runtime State (Mutable)
```python
external_msg_rate_state = {}           # Rate limit tracking
external_target_rate_state = {}        # Per-target rate limits
PENDING_TIMEZONE_PROMPTS = {}          # Awaiting timezone input
SCHEDULED_MESSAGES = {}                # Pending scheduled DMs
PROXY_MESSAGE_AUDIT = {}               # {proxied_message_id: metadata}
```

#### Bot Instance
```python
bot = commands.Bot(
    command_prefix=commands.when_mentioned_or("Cor;", "cor;"),
    intents=intents,
    help_command=None,
    tree_cls=CortexCommandTree,
)
tree = bot.tree  # Slash command tree
```

---

### 3. **data.py** — Data Persistence & GitHub Sync

**Purpose**: Load, save, and sync system data with GitHub.

**Key Data Structure**:
```python
systems_data = {
    "systems": {
        "system_id": {
            "name": str,
            "owner_id": int,
            "icon": str | None,
            "description": str,
            "subsystems": {
                "subsystem_id": {
                    "name": str,
                    "description": str,
                    "members": {
                        "member_id": {member_data}
                    }
                }
            },
            "members": {member_data},  # Top-level members
            "external_messages": {
                "accept": bool,
                "blocked_users": [int],
                "muted_users": [int],
                "trusted_only": bool,
                "trusted_users": [int],
                "inbox": [{message_data}],
                "audit": [{audit_data}],
                ...
            },
            "groups": {group_data},
            "tags": {tag_data},
            ...
        }
    },
    "_moderation": {
        "reports": [...],
        "sanctions": {...},
        "events": [...]
    }
}
```

**Key Functions**:

- `_github_get_file(filename)` → `(data_dict, sha_hash)`
  - Fetches file from GitHub API
  - Returns JSON content and SHA for updates

- `_github_save_file(filename, data_obj)` → None
  - Pushes JSON data to GitHub
  - Uses authenticated PUT request

- `save_systems()` → None
  - Saves `systems_data` to local JSON file
    - Queues GitHub persistence in a background worker if GITHUB_TOKEN is set

**Initialization**:
1. Check if `cortex_members.json` exists locally
2. If not, try loading from GitHub on fresh deploy
3. Fall back to empty structure if neither exists

---

### 4. **helpers.py** — Core Utility Library (~2000+ lines)

**Purpose**: Centralized utility functions for all operations.

#### A. **Moderation Helpers**
```python
get_moderation_state()                   # Get _moderation dict
get_user_sanctions(user_id)              # Warnings, bans, suspensions
is_user_suspended(user_id, scope)        # Check suspension status
is_bot_moderator_user(user_id)           # Check if user is bot owner/admin
ensure_moderator(interaction)            # Verify permission, send error if not
add_mod_event(action, target_id, mod_id, details)  # Log moderation action
```

#### B. **System & Member Lookup**
```python
get_user_system_id(user_id)              # Find system ID for owner
get_system_members(system_id, subsystem_id=None)  # Get members dict
get_user_mode(user_id)                   # "system" or "singlet"
get_scope_label(subsystem_id)            # Get display name for subsystem/main
iter_system_member_dicts(system)         # Yield (scope_id, members_dict) tuples
resolve_member_identifier(members, token)           # Find member by ID/name/nickname
resolve_member_identifier_in_system(system, token, subsystem_id)  # With system context
```

#### C. **Command Gate Checks**
```python
@prefix_command_gate                     # Decorator for prefix commands
async def _global_interaction_check(interaction)  # Decorator for slash commands

# Checks:
# - Moderator-only commands
# - User suspension/bans
# - Singlet mode restrictions
# - Command availability
```

#### D. **Group Management**
```python
get_group_settings(system)               # Get groups dict with defaults
get_next_group_id(system)                # Generate unique group ID
get_group_path_text(groups, group_id)    # Format group hierarchy path
sorted_group_ids_for_member(system, member)  # Groups a member belongs to
format_member_group_lines(system, member)    # Format groups for display
build_group_order_embed(system, order, focus_index)  # UI embed for reordering
```

#### E. **Member Management**
```python
get_next_system_member_id(system_id)     # Generate unique member ID
resolve_member_for_override(system, member_id, subsystem_id)  # Get with overrides
get_member_sort_mode(system)             # "alphabetical", "creation", etc.
sort_member_rows(member_rows, sort_mode)  # Sort members for display
get_member_lookup_keys(member)           # ID, name, nickname alternatives
get_server_member_appearance(system, guild_id, member_id)  # Server-specific override
move_member_between_scopes(system_id, member_id, to_subsystem_id, from_subsystem_id)
```

#### F. **Proxy & Fronting**
```python
get_fronting_member_for_user(user_id)    # Get currently fronting member
get_member_proxy_formats(member_data)    # Get proxy tag list
set_member_proxy_formats(member_data, proxy_formats)  # Update proxy tags
add_member_proxy_format(member_data, prefix, suffix)
remove_member_proxy_format(member_data, prefix, suffix)
get_member_proxy_parts(member_data)      # [{"prefix": str, "suffix": str}, ...]
find_tagged_proxy_member(system, content)  # Find member by proxy tag

start_front(system, member_id, subsystem_id=None)  # Start fronting
end_front(system, member_id)             # Stop fronting
calculate_front_duration(member)         # Get current front duration
time_of_day_bucket(datetime)             # "morning", "afternoon", "evening", "night"
```

#### G. **Autoproxy & Settings**
```python
get_autoproxy_settings(system)           # Global autoproxy config
get_server_autoproxy_settings(system, guild_id, create=False)  # Server-specific
get_effective_autoproxy_settings(system, guild_id)  # Combined settings
apply_autoproxy_mode(settings, mode)     # Apply autoproxy mode logic
```

#### H. **Timezone Management**
```python
get_system_timezone_name(system)         # Get system timezone string
normalize_timezone_name(raw_name)        # Convert alias to canonical name
get_system_timezone(system)              # Get ZoneInfo object
async timezone_autocomplete(interaction, current)  # Slash command autocomplete
```

#### I. **Tags & System Profile**
```python
get_system_profile(system)               # Profile data with defaults
get_system_proxy_tag(system)             # System-wide proxy tag
get_system_tag_list(system, create=False)  # System custom tags
get_available_tags_for_system(system)    # Preset + custom tags
add_custom_tags_to_system(system, tags)  # Add custom tags
normalize_tag_value(tag)                 # Clean/normalize tag string
```

#### J. **External Messaging**
```python
get_external_settings(system)            # Get external_messages with defaults
add_external_audit_entry(system, action, sender_user_id, target_member_id, details)
is_in_quiet_hours(system, now_utc=None)  # Check quiet hours setting
prune_external_temp_blocks(settings)     # Remove expired temp blocks
cleanup_external_inbox_entries(system)   # Remove old inbox messages
check_and_update_external_rate_limit(sender_user_id)  # Rate limit check
check_and_update_external_target_rate_limit(sender, target, seconds)
format_inbox_entry_for_channel(msg)      # Format for display in inbox channel
format_inbox_entry_for_dm(msg)           # Format for DM notification
```

#### K. **Privacy & Access Control**
```python
normalize_profile_privacy_level(value, default="private")  # Normalize level
get_system_privacy_level(system)         # System privacy level
get_member_privacy_level(member)         # Member privacy level
can_view_with_privacy(level, system, viewer_user_id)  # Check access
can_view_system_data(system, viewer_user_id)
can_view_member_data(system, member, viewer_user_id)
resolve_target_system_for_view(requester_user_id, target_user_id_raw=None)
```

#### L. **Focus Modes**
```python
get_focus_modes(system)                  # Get focus mode settings
get_all_mode_names(system)               # List all mode names
start_focus_mode(system, mode_name)      # Start a focus mode
end_focus_mode(system)                   # End current mode
calc_mode_stats(system, days=30)         # Calculate mode time stats
```

#### M. **Wellness & Check-in**
```python
get_checkin_settings(system)             # Check-in config
get_checkin_trend_text(system)           # Trend summary ("↓ declining", etc.)
build_weekly_checkin_summary(system)     # Build embed for weekly summary
current_week_key(now=None)               # Key for current week's data
get_front_reminder_settings(system)      # Front reminder config
```

#### N. **Embeds & Display**
```python
build_member_profile_embed(member, system=None)  # Member card embed
build_subsystem_card_embed(subsystem_data, subsystem_id, system)  # Subsystem card
normalize_embed_image_url(value)         # Clean image URLs
is_ephemeral_discord_attachment_url(value)  # Check if URL expires
normalize_playlist_link(url)              # Format playlist links
```

#### O. **PluralKit Import**
```python
async fetch_pluralkit_members(token)     # Get PK members from API
async fetch_pluralkit_system(token)      # Get PK system from API
map_pluralkit_member_to_cortex(pk_member)  # Convert PK format to Cortex
get_pk_proxy_formats(proxy_tags)         # Extract proxy formats from PK
```

#### P. **Utility Functions**
```python
parse_bool_token(value)                  # Parse "true"/"false"/"yes"/"no"
parse_discord_user_id(raw_value)         # Extract <@123> or 123
parse_time_delta(time_str)               # Parse "1d5h30m" to seconds
parse_sendmessage_args(args_str)         # Parse special message syntax
normalize_hex(hex_code)                  # Validate hex color code
add_scheduled_message(user_id, message, time_delta)  # Queue DM
format_duration(seconds)                 # "3h 45m 12s"
migrate_system_member_ids(system)        # Data migration helper
normalize_proxy_formats(proxy_formats)   # Validate proxy format list
```

#### Q. **Autocomplete Functions**
```python
member_name_autocomplete(interaction, current)
member_id_autocomplete(interaction, current)
switchmember_member_id_autocomplete(interaction, current)
subsystem_id_autocomplete(interaction, current)
tag_autocomplete(interaction, current)
timezone_autocomplete(interaction, current)
```

---

### 5. **commands_slash.py** — Slash Commands

**Purpose**: Register and handle `/` slash commands via Discord's app_commands API.

**Key Command Categories**:

#### Registration & System Setup
- `/register` — Create new system
- `/registersystem` — Alias for register
- `/allowexternal` — Enable external messaging
- `/importfrompluralkit` — Import system from PluralKit

#### System Cards & Identity
- `/systemcard` — View system profile card
- `/editsystemcard` — Edit system info
- `/systemicon` — Set system icon
- `/systemcolor` — Set system accent color
- `/systemprivacy` — Set system privacy level

#### Member Management
- `/addmember` — Add new member to system
- `/editmember` — Edit member profile
- `/removemember` — Delete member
- `/switchmember` — Change fronting member
- `/memberprivacy` — Set member privacy level
- `/membercard` — View member profile
- `/membericon` — Set member icon
- `/altericon` — Set member icon (alias)
- `/alternicnames` — Set member nicknames
- `/bannedusers` — List banned users (moderation)

#### Subsystems (Plural within System)
- `/addsubsystem` — Create subsystem
- `/editsubsystem` — Edit subsystem
- `/removesubsystem` — Delete subsystem
- `/viewsubsystemcard` — View subsystem card

#### Proxy Tags & Listing
- `/addproxytag` — Add proxy format
- `/removeproxytag` — Remove proxy format
- `/memberlist` — List all members
- `/listmembers` — Alias for memberlist

#### Groups (Organizing Members)
- `/addgroup` — Create group
- `/editgroup` — Edit group
- `/removegroup` — Delete group
- `/assigngroup` — Add member to group
- `/unassigngroup` — Remove member from group
- `/reordergroups` — Interactive UI to reorder groups

#### Tags (Categorization)
- `/addtag` — Add tag to member
- `/removetag` — Remove tag from member
- `/showtags` — Show all available tags
- `/customtags` — Manage custom tags

#### Fronting & Autoproxy
- `/switchmember` — Change front
- `/startfront` — Mark member as fronting
- `/endfront` — Mark member as not fronting
- `/currentfront` — Show who's currently fronting
- `/setautoproxy` — Configure autoproxy
- `/autoproxy` — View autoproxy settings

#### Front History & Stats
- `/systemstats` — System activity statistics
- `/frontlog` — Show fronting history
- `/frontreminder` — Configure front reminders

#### Check-in & Wellness
- `/checkin` — Record mood check-in
- `/checkinstatus` — View check-in trend
- `/weeklymoodsummary` — Weekly mood summary
- `/settimezone` — Set system timezone
- `/timezonestatus` — View timezone

#### Focus Mode (Singlet Feature)
- `/setmode` — Start focus mode
- `/endmode` — Exit focus mode
- `/currentmode` — Show active mode
- `/modestats` — Mode time statistics

#### External Messaging
- `/sendexternal` — Send message to system member
- `/externalprivacy` — Set external inbox privacy
- `/externalstatus` — View external settings
- `/trustedusers` — Manage trusted users
- `/externallimits` — View rate limits
- `/externalquiethours` — Set quiet hours (suppress notifications)
- `/externalretention` — Set inbox cleanup

#### Moderation
- `/modappeal` — Appeal suspension/ban
- `/modreports` — View user reports
- `/modwarn` — Warn user
- `/modsuspend` — Suspend user
- `/modban` — Ban user from bot
- `/modunban` — Remove ban

#### Maintenance
- `/synccommands` — Sync commands to Discord
- `/refresh` — Force reload data from GitHub
- `/reportexternal` — Report external message abuse

---

### 6. **commands_prefix.py** — Prefix Commands

**Purpose**: Handle text-based prefix commands using `Cor;` or `cor;` prefix.

**Structure**: Mirror of slash commands, but triggered by text prefix.

**Example**:
```
Cor;memberlist           → List all members
Cor;addmember Alice      → Add member named Alice
Cor;switchmember Bob     → Switch to Bob
Cor;sendexternal @user Hello  → Send DM
```

**Key Differences from Slash Commands**:
- Text-based input parsing
- No autocomplete UI
- More flexible argument handling
- Prefix command gate applied

---

### 7. **events.py** — Discord Event Handlers

**Purpose**: Handle Discord bot lifecycle events and message processing.

#### `@bot.event on_ready()`
Triggered when bot connects and is ready.
- Logs login status
- Starts background task loops (front reminder, mood summary, scheduled messages)
- Skips command sync to avoid Cloudflare rate limits

#### `@bot.event on_message(message)`
Triggered for every message.
- Filters bot/webhook messages
- Handles pending timezone prompts (awaiting user input)
- Processes prefix commands via `bot.get_context()` and `bot.process_commands()`

**Timezone Prompt Flow**:
1. User runs `/settimezone`
2. Bot sets `PENDING_TIMEZONE_PROMPTS[user_id] = timeout_datetime`
3. User sends any message
4. If matches timezone pattern, save it and clear pending
5. If timeout expires, clear pending

---

### 8. **tasks.py** — Background Task Loops

**Purpose**: Run recurring background tasks every N minutes.

#### `@tasks.loop(minutes=5) front_reminder_loop()`
Checks if members have been fronting too long.
- Iterates all systems and members
- If fronting for >threshold hours, sends DM to system owner
- Sets `reminder_sent=True` to avoid duplicate DMs
- Saves state changes

#### `@tasks.loop(minutes=1) scheduled_messages_loop()`
Delivers queued scheduled DMs when time arrives.
- Checks `SCHEDULED_MESSAGES` registry
- If send time has passed, delivers message via bot DM
- Removes delivered messages from queue
- Handles failures gracefully

#### `@tasks.loop(minutes=1) weekly_mood_summary_loop()`
Sends weekly mood summaries at configured time.
- Checks each system's check-in settings
- If weekly send time has arrived, builds summary and sends DM
- Updates last-sent timestamp

**Startup Hook**: `@loop.before_loop` waits for `bot.wait_until_ready()` before starting.

---

### 9. **views.py** — Discord UI Components

**Purpose**: Interactive Discord UI views (buttons, select menus, modals).

#### `GroupOrderView(discord.ui.View)`
Interactive reordering UI for groups.

**Features**:
- **Focus Up/Down buttons**: Navigate between groups
- **Move Up/Down buttons**: Reorder groups in priority
- **Save button**: Commit new order to data
- **Cancel button**: Discard changes

**Interaction Check**: Only system owner can use buttons.

**Methods**:
- `_update_button_state()` — Disable buttons based on state
- `current_embed()` — Generate current display embed
- `on_timeout()` — Disable all buttons when 3min timeout expires

---

## Core Concepts

### 1. **System Structure**

A **system** belongs to one Discord user (owner) and contains:

```
System (owner_id = 123456)
├── Members
│   ├── Alice     (member_id: "a", name: "Alice", icon: "...")
│   └── Bob       (member_id: "b", name: "Bob", ...)
├── Subsystems
│   ├── Protectors System
│   │   ├── Members
│   │   │   ├── Guard1
│   │   │   └── Guard2
│   │   └── Settings
│   └── Littles (subsystem_id: "littles")
│       └── Members: Little1, Little2
├── Fronting (current_front: {member_id, cofronts[], start, ...})
├── Groups (organizational categories)
├── External Messaging (inbox, trusted users, blocked users, etc.)
├── Tags (custom categorization)
└── Settings (autoproxy, check-in, front reminder, focus modes, etc.)
```

### 2. **Member Privacy Levels**

Four-tier privacy model:

| Level | Owner | Friends | Trusted | Public | Description |
|-------|-------|---------|---------|--------|-------------|
| `private` | ✓ | ✗ | ✗ | ✗ | Owner only |
| `friends` | ✓ | ✓ | ✓ | ✗ | Owner + friends + trusted |
| `trusted` | ✓ | ✗ | ✓ | ✗ | Owner + trusted |
| `public` | ✓ | ✓ | ✓ | ✓ | Everyone |

**Implementation**: `can_view_member_data(system, member, viewer_user_id)` checks member privacy level.

### 3. **Proxy Formats**

Members can have multiple proxy formats (delimiters):

```
Member: Alice
Proxy Formats:
  - Prefix: "a:", Suffix: ""      → "a: message" proxies as Alice
  - Prefix: "*", Suffix: "*"      → "*message*" proxies as Alice
  - Prefix: "[alice]", Suffix: "" → "[alice] message" proxies as Alice
```

Proxy message detection:
1. Extract prefix + content + suffix from message
2. Find member matching proxy format
3. Send message via webhook as that member

### 4. **Fronting & Co-fronting**

Member alternates who's active ("fronting"):

```json
{
  "current_front": {
    "member_id": "a",              // Primary fronting member
    "cofronts": ["b", "c"],        // Additional members also fronting
    "start": "2025-03-19T10:30:00+00:00",
    "reminder_sent": false,
    "reminded_at": null
  }
}
```

### 5. **Scopes**

Members can exist in either:
- **Main scope** (system-level members)
- **Subsystem scopes** (members within a subsystem)

Scope ID = `None` for main, or subsystem_id string for subsystems.

---

## Data Structures

### System Object

```python
{
    "name": "plural system",
    "owner_id": 123456789,
    "icon": "https://...",
    "description": "System description",
    "color": "3498DB",
    "timezone": "America/New_York",
    "mode": "system",  # or "singlet"
    
    # Fronting
    "current_front": {
        "member_id": "a",
        "cofronts": [],
        "start": "2025-03-19T10:30:00+00:00",
        "reminder_sent": False,
        "reminded_at": None
    },
    
    # Members
    "members": {
        "a": { /* member object */ },
        "b": { /* member object */ }
    },
    
    # Subsystems
    "subsystems": {
        "protectors": {
            "name": "Protectors",
            "description": "...",
            "members": {
                "p1": { /* member object */ }
            }
        }
    },
    
    # Groups
    "groups": {
        "group1": {
            "name": "Switches",
            "description": "...",
            "color": "FF5733",
            "icon": "...",
            "members": ["a", "b"],
            "order": 1
        }
    },
    
    # Tags
    "tags": {
        "host": True,
        "protector": True,
        "fictive": False
    },
    "custom_tags": ["narrator", "gatekeeper"],
    
    # External messaging
    "external_messages": {
        "accept": True,
        "blocked_users": [999],
        "muted_users": [],
        "trusted_only": False,
        "trusted_users": [123, 456],
        "quiet_hours": {
            "enabled": False,
            "start": "22:00",
            "end": "08:00"
        },
        "temp_blocks": {
            "888": "2025-03-19T15:00:00"  # user_id: expires_at
        },
        "inbox": [
            {
                "id": "msg123",
                "from_user_id": 123,
                "member_id": "a",
                "message": "Hello!",
                "timestamp": "2025-03-19T...",
                "to_dm": True,
                "to_channel": False,
                "channel_id": None
            }
        ],
        "audit": [
            {
                "timestamp": "2025-03-19T...",
                "action": "message_sent",
                "sender_user_id": 123,
                "target_member_id": "a",
                "details": {...}
            }
        ]
    },
    
    # Autoproxy
    "autoproxy": {
        "enabled": False,
        "mode": "off",      # "off", "front", "member", "latch"
        "last_member": "a"
    },
    "server_autoproxies": {
        "guild_123": {
            "enabled": False,
            "mode": "off",
            "last_member": "a"
        }
    },
    
    # Proxy tag
    "proxy_tag": "[member]",
    
    # Settings
    "settings": {
        "member_sort_mode": "alphabetical"  # or "creation"
    },
    
    # Check-in
    "checkins": {
        "enabled": True,
        "send_at": "09:00",        # Time of day to send reminder
        "last_sent_week": "2025-W12",
        "data": {
            "2025-03-19": [
                {
                    "timestamp": "2025-03-19T09:30:00",
                    "rating": 7,
                    "note": "Good day"
                }
            ]
        }
    },
    
    # Focus modes (singlet)
    "focus_modes": {
        "studying": {...},
        "gaming": {...}
    },
    "current_focus": {
        "mode": "studying",
        "start": "2025-03-19T...",
        "sessions": [...]
    },
    
    # Front reminders
    "front_reminders": {
        "enabled": True,
        "hours": 4
    }
}
```

### Member Object

```python
{
    "name": "Alice",
    "member_id": "a",
    "privacy_level": "private",  # or "trusted", "friends", "public"
    "description": "Main fronter",
    "pronouns": "she/her",
    "icon": "https://...",
    "color": "FF5733",
    "birthday": "01-15",
    
    # Proxy
    "proxy_formats": [
        {"prefix": "a:", "suffix": ""},
        {"prefix": "[alice]", "suffix": ""}
    ],
    
    # Fronting
    "current_front": {
        "member_id": "a",
        "start": "2025-03-19T10:30:00+00:00",
        "cofronts": [],
        "reminder_sent": False,
        "reminded_at": None
    },
    
    # History
    "front_history": [
        {
            "start": "2025-03-19T10:00:00",
            "end": "2025-03-19T15:00:00",
            "cofronts": []
        }
    ],
    
    # Tags
    "tags": ["host", "protector"],
    
    # Server overrides
    "server_names": {
        "guild_123": "Alice in Guild"  # Override name in specific server
    },
    "server_icons": {
        "guild_123": "https://..."
    },
    
    # Settings
    "settings": {
        "nicknames": ["Ali", "A"],
        "playlist": "https://spotify.com/..."
    }
}
```

### External Message (Inbox Entry)

```python
{
    "id": "msg_unique_id",
    "from_user_id": 123456,              # Who sent it
    "member_id": "a",                    # Target member
    "message": "Hello!",
    "timestamp": "2025-03-19T10:30:00+00:00",
    "to_dm": True,                       # Send to system owner as DM
    "to_channel": False,                 # Send to configured inbox channel
    "channel_id": None                   # If to_channel, which channel
}
```

---

## Key Helper Functions

### Privacy & Access Control

```python
can_view_member_data(system, member, viewer_user_id) -> bool
# Returns: Owner can always view
#          Public members visible to everyone
#          Trusted members visible to trusted list
#          Friends members visible to friends/trusted list
#          Private members only visible to owner

can_view_system_data(system, viewer_user_id) -> bool
# Wrapper for system-level privacy check

can_view_with_privacy(level, system, viewer_user_id) -> bool
# Core implementation: checks privacy level vs viewer
```

### Member Resolution

```python
resolve_member_identifier_in_system(system, token, subsystem_id=None) -> (member, error)
# Resolves token like "a", "Alice", "ali" to member object
# Tries: exact ID → exact name → nickname → partial name
# Returns (member_obj, None) or (None, error_message)

get_member_from_scope(system, scope_id, member_id) -> member
# Get member from main scope (None) or subsystem scope
```

### Fronting

```python
start_front(system, member_id, subsystem_id=None)
# Marks member as currently fronting
# Clears any existing front in that scope

end_front(system, member_id)
# Clears member from front if they're currently fronting
# Records front to history

calculate_front_duration(member) -> seconds
# Returns how long member has been fronting (or None)
```

### Proxy Resolution

```python
find_tagged_proxy_member(system, content) -> (member, proxy_format)
# Scans content for proxy format matches
# Returns matching member and which format matched
# Returns (None, None) if no match

get_member_proxy_formats(member) -> [{prefix, suffix}, ...]
# Returns member's proxy tag list

get_member_proxy_parts(member) -> {prefix, suffix}
# Returns first proxy format broken into parts
```

### Rate Limiting

```python
check_and_update_external_rate_limit(sender_user_id) -> bool
# Check 5 messages per 60 seconds per user
# Returns False if limit exceeded

check_and_update_external_target_rate_limit(sender, target, seconds) -> bool
# Check per-target cooldown (prevent spam to single member)
```

---

## Command Structure

### Slash Command Pattern

```python
@tree.command(name="addmember", description="Add a new member")
async def addmember(
    interaction: discord.Interaction,
    name: str,
    pronouns: str = "they/them"
):
    await interaction.response.defer()
    
    # Get system
    system_id = get_user_system_id(interaction.user.id)
    if not system_id:
        await interaction.followup.send("You don't have a system registered.")
        return
    
    system = systems_data["systems"][system_id]
    
    # Create member
    member_id = get_next_system_member_id(system_id)
    system["members"][member_id] = {
        "name": name,
        "member_id": member_id,
        "pronouns": pronouns,
        # ... other fields
    }
    
    save_systems()
    await interaction.followup.send(f"Added **{name}** to your system!")
```

### Prefix Command Pattern

```python
@bot.command(name="addmember")
async def addmember_prefix(ctx, name, pronouns="they/them"):
    system_id = get_user_system_id(ctx.author.id)
    if not system_id:
        await ctx.send("You don't have a system registered.")
        return
    
    system = systems_data["systems"][system_id]
    member_id = get_next_system_member_id(system_id)
    system["members"][member_id] = {...}
    save_systems()
    
    await ctx.send(f"Added **{name}** to your system!")
```

### Autocomplete Pattern

```python
async def member_name_autocomplete(interaction, current):
    system_id = get_user_system_id(interaction.user.id)
    if not system_id:
        return []
    
    members = get_system_members(system_id)
    return [
        app_commands.Choice(name=m["name"], value=m["member_id"])
        for m in members.values()
        if m["name"].lower().startswith(current.lower())
    ][:25]  # Max 25 choices

@tree.command()
async def switchmember(
    interaction: discord.Interaction,
    member: str
):
    # Autocomplete provides member_id in 'member' parameter
    ...
```

---

## Configuration and Environment

### Environment Variables (.env file)

```bash
# Discord
DISCORD_BOT_TOKEN=your_bot_token_here

# Admin/Owner
CORTEX_ADMIN_USER_ID=your_discord_id
CORTEX_INSTANCE_LABEL=Production     # Optional: tag for bot instance

# GitHub Backup
GITHUB_TOKEN=your_github_token
GITHUB_REPO=yourusername/PythonTest  # Optional, defaults to salemtheweaver/PythonTest
```

### Constants (config.py)

See [config.py section](#2-configpy--configuration--constants) for full list.

### Data Files

- **cortex_members.json** — Main system data (loaded/saved)
- **tags.json** — System tag presets (optional)

---

## Privacy Model

### Four-Tier System

1. **Private** — Owner only
   - Use case: Very sensitive member info
   - Visibility: Only system owner

2. **Friends** — Owner + friends + trusted users
   - Use case: Close friends can see all details
   - Visibility: Owner + anyone in friend list + anyone in trusted list

3. **Trusted** — Owner + trusted users only
   - Use case: Limited inner circle
   - Visibility: Owner + anyone in trusted list

4. **Public** — Everyone
   - Use case: Member is public
   - Visibility: Everyone

### Implementation

```python
def can_view_member_data(system, member, viewer_user_id):
    owner_id = system.get("owner_id")
    if str(viewer_user_id) == str(owner_id):
        return True  # Owner always views own members
    
    level = member.get("privacy_level", "private")
    return can_view_with_privacy(level, system, viewer_user_id)

def can_view_with_privacy(level, system, viewer_user_id):
    if level == "public":
        return True
    
    settings = get_external_settings(system)
    trusted_ids = set(str(uid) for uid in settings.get("trusted_users", []))
    friends_ids = set(str(uid) for uid in settings.get("friend_users", []))
    
    viewer_str = str(viewer_user_id)
    
    if level == "trusted":
        return viewer_str in trusted_ids
    
    if level == "friends":
        return viewer_str in (friends_ids | trusted_ids)  # Friends includes trusted
    
    return False  # private
```

---

## External Messaging System

### Features

- **Accept/reject** external messages from non-system-members
- **Inbox** — Collect external messages with notifications
- **Quiet hours** — Suppress notifications during certain times
- **Blocked/muted users** — Block user or mute notifications
- **Trusted users** — Only accept from trusted list
- **Temporary blocks** — Auto-expire blocks
- **Rate limiting** — 5 messages per 60 seconds, per-target cooldown
- **Privacy gates** — Can't message member if privacy blocks viewing
- **Audit trail** — Track all external activity

### Flow

1. External user sends `/sendexternal @system_owner Hello Alice`
2. Check if system accepts external messages
3. Check if user is blocked
4. Check if user is trusted (if trusted_only enabled)
5. Check privacy level of target member
6. Check rate limits
7. Store message in inbox
8. Notify system owner (DM or channel)
9. Add audit entry

### Settings

```python
external_settings = {
    "accept": True,                    # Accept external messages
    "blocked_users": [999, 888],       # Blocked user IDs
    "muted_users": [777],              # Muted (no notifications)
    "trusted_only": False,             # Only from trusted users
    "trusted_users": [123, 456],       # Allowed users if trusted_only
    "quiet_hours": {
        "enabled": True,
        "start": "22:00",              # 10 PM
        "end": "08:00"                 # 8 AM
    },
    "temp_blocks": {
        "888": "2025-03-19T15:00:00"   # Expires at this ISO datetime
    },
    "inbox_retention_days": 30,        # Auto-delete old messages
    "inbox": [...],                    # Message list
    "audit": [...]                     # Activity log
}
```

---

## Development Guidelines

### Adding a New Command

1. **Define in slash commands**:
   ```python
   # commands_slash.py
   @tree.command(name="newcommand", description="Does something")
   async def newcommand(interaction: discord.Interaction, arg: str):
       await interaction.response.defer()
       # Implementation
       await interaction.followup.send("Done!")
   ```

2. **Mirror in prefix commands**:
   ```python
   # commands_prefix.py
   @bot.command(name="newcommand")
   async def newcommand_prefix(ctx, arg):
       # Same implementation
       await ctx.send("Done!")
   ```

3. **Add to appropriate command lists** if needed:
   - `SINGLET_ALLOWED_COMMANDS` if available in singlet mode
   - `MOD_COMMANDS` if moderation-only

### Adding a New Helper Function

1. Add to `helpers.py`
2. Import at top of command files as needed
3. Document with docstring
4. Consider edge cases (missing data, invalid input)

### Saving Data

Always call `save_systems()` after modifying `systems_data`:
```python
systems_data["systems"][system_id]["members"][m_id]["name"] = "New Name"
save_systems()  # Saves locally immediately and syncs GitHub in the background
```

### Error Handling

Always use `defer()` for commands that might take >3 seconds:
```python
await interaction.response.defer()
try:
    # Long operation
    result = some_slow_function()
    await interaction.followup.send(result)
except Exception as e:
    await interaction.followup.send(f"Error: {e}")
```

---

## Testing & Debugging

### Enable Debug Logging

```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

### Common Issues

| Issue | Check |
|-------|-------|
| Command not appearing | Run `/synccommands` to sync to Discord |
| Rate limit errors | Check Cloudflare limits, use /refresh instead |
| Data not saving | Check `GITHUB_TOKEN` and file permissions |
| Timezone errors | Validate timezone name with `normalize_timezone_name()` |
| Member not found | Check subsystem_id and exact member ID |

---

## Deployment

1. **Local testing**: `python cortex.py`
2. **Environment setup**: Create `.env` with tokens
3. **Install dependencies**: `pip install -r requirements.txt`
4. **Deploy**: Run bot as background process or container
5. **GitHub backup**: Configure `GITHUB_TOKEN` for data persistence
6. **Command sync**: Run `/synccommands` in Discord after changes

---

## Version & Dependency Information

**Current Version**: 2.0.0  
**Last Updated**: March 19, 2026  
**Bot Architecture**: Modular (9-file structure)  
**Python Version**: 3.8+  
**Discord.py**: 2.7.1  

### Dependencies
```
discord.py==2.7.1
python-dotenv==1.1.0
pytz==2025.2
```

### Repository
- **Local Path**: `c:\Users\alext\OneDrive\Desktop\PythonTest`
- **Remote Repo**: salemtheweaver/PythonTest (GitHub)
- **Data Backup**: GitHub (via GITHUB_TOKEN)

### Quick Links
- [Version Control](#version-control)
- [Architecture](#architecture)
- [Module Breakdown](#module-breakdown)
- [Configuration](#configuration-and-environment)

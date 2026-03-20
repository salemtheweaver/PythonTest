# cortex.py — Entry point for the Cortex Discord bot
#
# This file wires together all modules and starts the bot.
# The actual logic lives in:
#   config.py          — Constants, env vars, bot instance
#   data.py            — Data persistence, GitHub sync
#   helpers.py         — Utility/helper functions
#   views.py           — Discord UI views
#   commands_slash.py  — Slash commands
#   commands_prefix.py — Prefix commands
#   events.py          — Event handlers
#   tasks.py           — Background task loops

from config import bot, TOKEN, CortexCommandTree

# Patch the CortexCommandTree interaction check now that helpers are available
from helpers import _global_interaction_check
CortexCommandTree.interaction_check = lambda self, interaction: _global_interaction_check(interaction)

# Import modules to register commands, events, and tasks with the bot
import commands_slash
import commands_prefix
import events
import tasks

# Also register the prefix command gate from helpers
from helpers import prefix_command_gate

# Debug print to confirm bot instance and token
bot.run(TOKEN)

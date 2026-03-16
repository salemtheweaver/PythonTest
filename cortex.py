import discord
from discord import app_commands
from discord.ext import commands
import json
import os
from dotenv import load_dotenv
import random
from datetime import datetime, timedelta, timezone, UTC
import pytz

# Load Discord bot token securely from environment variable
load_dotenv()
TOKEN = os.getenv("DISCORD_BOT_TOKEN")
if not TOKEN:
    raise RuntimeError("DISCORD_BOT_TOKEN environment variable not set.")
JSON_FILE = "cortex_members.json"
TAGS_FILE = "tags.json"

intents = discord.Intents.default()
intents.message_content = True

bot = commands.Bot(command_prefix="!", intents=intents)
tree = bot.tree

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
    with open(TAGS_FILE, "w") as f:
        json.dump([], f, indent=4)

with open(TAGS_FILE, "r") as f:
    PRESET_TAGS = json.load(f)

def save_tags():
    with open(TAGS_FILE, "w") as f:
        json.dump(PRESET_TAGS, f, indent=4)

def save_members():
    with open(JSON_FILE, "w") as f:
        json.dump(members, f, indent=4)

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
        ]
        super().__init__(
            placeholder="Select tags...",
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
        options = [discord.SelectOption(label=tag, value=tag) for tag in PRESET_TAGS]
        super().__init__(
            placeholder="Select one or more tags to filter...",
            min_values=1,
            max_values=len(options),
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

def start_front(member_id, cofronts=None):
    """Start fronting for a member with optional co-fronts."""
    if cofronts is None:
        cofronts = []

    member = members[member_id]
    now_iso = datetime.now(timezone.utc).isoformat()

    # End any existing front
    if member.get("current_front"):
        end_front(member_id)

    # Set current front
    member["current_front"] = {"start": now_iso, "cofronts": cofronts}

    # Ensure front history exists
    member.setdefault("front_history", [])
    # Add new session to history (end=None for ongoing)
    member["front_history"].append({"start": now_iso, "end": None, "cofronts": cofronts})

def end_front(member_id):
    """End current front session for a member and record duration."""
    member = members[member_id]
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

# -----------------------------
# Co-front UI
# -----------------------------
class CoFrontSelect(discord.ui.Select):
    def __init__(self, main_member_id):
        options = [
            discord.SelectOption(label=m["name"], value=m["id"])
            for m in members.values() if m["id"] != main_member_id
        ]
        super().__init__(
            placeholder="Select co-front members...",
            min_values=0,
            max_values=len(options),
            options=options
        )

    async def callback(self, interaction: discord.Interaction):
        self.view.selected_cofronts = self.values
        await interaction.response.defer()

class CoFrontView(discord.ui.View):
    def __init__(self, main_member_id):
        super().__init__(timeout=120)
        self.selected_cofronts = []
        self.add_item(CoFrontSelect(main_member_id))
        self.add_item(ConfirmTags())

# -----------------------------
# Add member
# -----------------------------
@tree.command(name="addmember", description="Add a member")
@app_commands.default_permissions(administrator=True)
async def addmember(
    interaction: discord.Interaction,
    name: str,
    pronouns: str = None,
    birthday: str = None,
    description: str = None,
    profile_pic: discord.Attachment = None,
    banner: discord.Attachment = None,
    yt_playlist: str = None,
    color: str = None
):
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

    # Generate next incremental member ID
    if members:
        numeric_ids = []
        for m in members.values():
            try:
                numeric_ids.append(int(m["id"]))
            except:
                continue
        member_id = str(max(numeric_ids, default=0) + 1)
    else:
        member_id = "1"

    # Create the member object
    members[member_id] = {
        "id": member_id,
        "name": name,
        "pronouns": pronouns,
        "birthday": birthday,
        "description": description,
        "profile_pic": profile_pic.url if profile_pic else None,
        "banner": banner.url if banner else None,
        "yt_playlist": yt_playlist,
        "tags": tags_list,
        "color": color_hex,
        "created_at": datetime.now(UTC).isoformat(),  # track creation date
        "fronting": False,
        "cofronting": [],
        "fronting_since": None,
        "front_history": []
    }

    # Save members file
    save_members()

    # Confirm addition to user
    await interaction.followup.send(f"Member **{name}** added.\nID `{member_id}`", ephemeral=True)

# -----------------------------
# Switch member
# -----------------------------
@tree.command(name="messageto", description="Send a message to a member that shows when they switch in")
@app_commands.autocomplete(member_id=member_id_autocomplete)
async def messageto(interaction: discord.Interaction, member_id: str, message: str):
    if member_id not in members:
        await interaction.response.send_message("Member not found.", ephemeral=True)
        return

    # Find who is currently fronting to use as sender
    sender_names = [m["name"] for m in members.values() if m.get("current_front")]
    sender = ", ".join(sender_names) if sender_names else "Unknown"

    # Add message to the member's inbox
    members[member_id].setdefault("inbox", [])
    members[member_id]["inbox"].append({
        "from": sender,
        "text": message,
        "sent_at": datetime.now(timezone.utc).isoformat()
    })
    save_members()

    await interaction.response.send_message(
        f"Message sent to **{members[member_id]['name']}**.",
        ephemeral=True
    )

# -----------------------------
# Switch member
# -----------------------------
@tree.command(name="switchmember", description="Log a member as fronting")
@app_commands.autocomplete(member_id=member_id_autocomplete)
async def switchmember(interaction: discord.Interaction, member_id: str):
    if member_id not in members:
        await interaction.response.send_message("Member not found.", ephemeral=True)
        return

    # End all currently fronting members first
    for m in members.values():
        if m.get("current_front"):
            end_front(m["id"])

    # Start fronting the selected member
    start_front(member_id)

    # Build response
    new_name = members[member_id]["name"]
    response = f"Member **{new_name}** is now fronting."

    # Show and clear any pending inbox messages
    inbox = members[member_id].get("inbox", [])
    if inbox:
        response += f"\n\n📨 **You have {len(inbox)} message(s):**"
        for msg in inbox:
            response += f"\n> **From {msg['from']}:** {msg['text']}"
        members[member_id]["inbox"] = []

    save_members()
    await interaction.response.send_message(response)

# -----------------------------
# Co-front member
# -----------------------------
@tree.command(name="cofrontmember", description="Select co-fronting members interactively")
@app_commands.autocomplete(member_id=member_id_autocomplete)
async def cofrontmember(interaction: discord.Interaction, member_id: str):
    if member_id not in members:
        await interaction.response.send_message("Member not found.", ephemeral=True)
        return

    view = CoFrontView(member_id)
    await interaction.response.send_message(
        f"Select co-front members for **{members[member_id]['name']}** then click Confirm.",
        view=view,
        ephemeral=True
    )
    await view.wait()

    # End any current fronting sessions
    for m in members.values():
        if m.get("current_front"):
            end_front(m["id"])

    # Start the front session with co-fronts
    start_front(member_id, cofronts=view.selected_cofronts)
    save_members()

    co_names = ", ".join([members[c]["name"] for c in view.selected_cofronts]) if view.selected_cofronts else "None"
    await interaction.followup.send(
        f"Member **{members[member_id]['name']}** is now fronting with co-fronts: {co_names}",
        ephemeral=True
    )

# -----------------------------
# Current fronts
# -----------------------------
@tree.command(name="currentfronts", description="View current front and co-fronts with durations")
async def currentfronts(interaction: discord.Interaction):
    fronters = []

    for m in members.values():
        current = m.get("current_front")
        if current:
            start_dt = datetime.fromisoformat(current["start"])
            duration = format_duration((datetime.now(timezone.utc) - start_dt).total_seconds())
            cofront_names = [members[c]["name"] for c in current.get("cofronts", []) if c in members]
            cofront_str = f" — Co-fronting: {', '.join(cofront_names)}" if cofront_names else ""
            fronters.append(f"• {m['name']} — {duration}{cofront_str}")

    embed = discord.Embed(
        title="Current Fronts",
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
@tree.command(name="fronthistory", description="View recent front history")
async def fronthistory(interaction: discord.Interaction):

    history_entries = []

    # Collect all front sessions
    for m in members.values():
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
            title="Front History",
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
@tree.command(name="frontstats", description="Show front duration statistics for all members")
async def frontstats(interaction: discord.Interaction):

    stats = []
    for m in members.values():
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
            title="Front Duration Statistics",
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
# -----------------------------
# Member statistics
# -----------------------------
@tree.command(name="memberstats", description="View fronting statistics for a member")
@app_commands.autocomplete(name=member_name_autocomplete)
async def memberstats(interaction: discord.Interaction, name: str):

    member = next((m for m in members.values() if m["name"].lower() == name.lower()), None)

    if not member:
        await interaction.response.send_message("Member not found.", ephemeral=True)
        return

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
@app_commands.autocomplete(name=member_name_autocomplete)
async def memberhistory(interaction: discord.Interaction, name: str):
    await interaction.response.defer()

    member = next((m for m in members.values() if m["name"].lower() == name.lower()), None)
    if not member:
        await interaction.followup.send("Member not found.")
        return

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
        end_dt = datetime.fromisoformat(end_iso) if end_iso else datetime.now(UTC)
        total_sec = int((end_dt - start_dt).total_seconds())
        hours, remainder = divmod(total_sec, 3600)
        minutes, seconds = divmod(remainder, 60)
        duration_str = f"{hours}h {minutes}m {seconds}s" if hours else f"{minutes}m {seconds}s"

        cofront_names = [members[c]["name"] for c in entry.get("cofronts", []) if c in members]
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

@tree.command(name="browsetags", description="Browse members by tags interactively")
async def browsetags(interaction: discord.Interaction):
    if not PRESET_TAGS:
        await interaction.response.send_message("No tags exist yet.", ephemeral=True)
        return
    embed = discord.Embed(title="Tag Browser", description="Select one or more tags from the dropdown.", color=discord.Color.blue())
    await interaction.response.send_message(embed=embed, view=TagMultiView())
# -----------------------------
# View member
# -----------------------------
@tree.command(name="viewmember", description="View a member profile")
@app_commands.autocomplete(name=member_name_autocomplete)
async def viewmember(interaction: discord.Interaction, name: str):

    member = next((m for m in members.values() if m["name"].lower() == name.lower()), None)

    if not member:
        await interaction.response.send_message("Member not found.", ephemeral=True)
        return

    # Build the description
    desc = member.get("description", "No description.")

    embed = discord.Embed(
        title=member["name"],
        description=desc,
        color=int(member.get("color", "FFFFFF"), 16)
    )

    if member.get("profile_pic"):
        embed.set_thumbnail(url=member["profile_pic"])

    embed.add_field(name="ID", value=member["id"], inline=True)
    embed.add_field(name="Pronouns", value=member.get("pronouns", "Unknown"), inline=True)
    embed.add_field(name="Birthday", value=member.get("birthday", "Unknown"), inline=True)

    tags = ", ".join(member.get("tags", [])) if member.get("tags") else "None"
    embed.add_field(name="Tags", value=tags, inline=False)

    if member.get("yt_playlist"):
        embed.add_field(name="Playlist", value=member["yt_playlist"], inline=False)

    fronting = "Yes" if member.get("fronting") else "No"
    embed.add_field(name="Currently Fronting", value=fronting, inline=False)

    if member.get("banner"):
        embed.set_image(url=member["banner"])

    # Show creation date under the banner in the footer
    created_iso = member.get("created_at")
    if created_iso:
        try:
            created_dt = datetime.fromisoformat(created_iso)
            created_formatted = created_dt.strftime("%B %d, %Y")
            embed.set_footer(text=f"Created At: {created_formatted}")
        except Exception:
            pass

    await interaction.response.send_message(embed=embed)
# -----------------------------
# Edit member
# -----------------------------
@tree.command(name="editmember", description="Edit member info")
@app_commands.autocomplete(member_id=member_id_autocomplete)
async def editmember(
    interaction: discord.Interaction,
    member_id: str,
    name: str = None,
    pronouns: str = None,
    birthday: str = None,
    description: str = None,
    yt_playlist: str = None,
    color: str = None,
    profile_pic: discord.Attachment = None,
    banner: discord.Attachment = None,
    edit_tags: bool = False
):
    if member_id not in members:
        await interaction.response.send_message("Member not found.", ephemeral=True)
        return

    member = members[member_id]

    if name:
        member["name"] = name
    if pronouns:
        member["pronouns"] = pronouns
    if birthday:
        member["birthday"] = birthday
    if description:
        member["description"] = description
    if yt_playlist:
        member["yt_playlist"] = yt_playlist
    if color:
        try:
            member["color"] = normalize_hex(color)
        except:
            await interaction.response.send_message("Invalid HEX color.", ephemeral=True)
            return
    if profile_pic:
        member["profile_pic"] = profile_pic.url
    if banner:
        member["banner"] = banner.url

    if edit_tags:
        view = TagView(preselected=member.get("tags", []))
        await interaction.response.send_message("Select tags then press Confirm.", view=view)
        timed_out = await view.wait()
        if not timed_out:
            member["tags"] = view.selected_tags
            save_members()
            await interaction.followup.send(f"Member **{member['name']}** updated.")
        else:
            await interaction.followup.send("Tag selection timed out. Other changes were still saved.")
            save_members()
    else:
        save_members()
        await interaction.response.send_message(f"Member **{member['name']}** updated.")
# -----------------------------
# Edit member images
# -----------------------------
@tree.command(name="editmemberimages", description="Edit a member's profile picture or banner")
@app_commands.autocomplete(member_id=member_id_autocomplete)
async def editmemberimages(
    interaction: discord.Interaction,
    member_id: str,
    profile_pic: discord.Attachment = None,
    banner: discord.Attachment = None
):
    if member_id not in members:
        await interaction.response.send_message("Member not found.", ephemeral=True)
        return

    member = members[member_id]

    if profile_pic:
        member["profile_pic"] = profile_pic.url
    if banner:
        member["banner"] = banner.url

    save_members()
    await interaction.response.send_message(f"Updated profile/banner for **{member['name']}**.")

# -----------------------------
# List all members
# -----------------------------
@tree.command(name="members", description="View all members in pages")
async def members_list(interaction: discord.Interaction):
    members_per_page = 15
    member_items = list(members.values())
    total_pages = (len(member_items) - 1) // members_per_page + 1

    page = 1  # start at page 1

    def get_embed(page):
        start_idx = (page - 1) * members_per_page
        end_idx = start_idx + members_per_page
        page_members = member_items[start_idx:end_idx]

        desc_lines = []
        for m in page_members:
            fronting = "Yes" if m.get("current_front") else "No"
            duration = format_duration(calculate_front_duration(m))
            co_fronts = ", ".join(m.get("co_fronts", [])) if m.get("co_fronts") else "None"
            desc_lines.append(
                f"**{m['name']}** | Fronting: {fronting} | Co-fronts: {co_fronts} | Total Front Time: {duration}"
            )

        embed = discord.Embed(
            title=f"Members List (Page {page}/{total_pages})",
            description="\n".join(desc_lines) or "No members found.",
            color=0x00FF00
        )
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

# -----------------------------
# Search Members
# -----------------------------
@tree.command(name="searchmember", description="Search for a member by name or tag")
@app_commands.autocomplete(name=member_name_autocomplete, tag=tag_autocomplete)
async def searchmember(interaction: discord.Interaction, name: str = None, tag: str = None):
    if not name and not tag:
        await interaction.response.send_message("Provide a name or tag to search.", ephemeral=True)
        return

    results = list(members.values())

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
@tree.command(name="addtag", description="Add a tag to the system")
async def addtag(interaction: discord.Interaction, tag: str):

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
@tree.command(name="listtags", description="List all system tags")
async def listtags(interaction: discord.Interaction):
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
@tree.command(name="clearfront", description="Clear fronting status without switching anyone")
async def clearfront(interaction: discord.Interaction):
    async def do_clear(interaction: discord.Interaction):
        any_fronting = False
        for m_id, m in members.items():
            if m.get("current_front"):
                end_front(m_id)
                any_fronting = True
        save_members()
        if any_fronting:
            await interaction.response.send_message("All members are now removed from front.", ephemeral=True)
        else:
            await interaction.response.send_message("No members were fronting.", ephemeral=True)

    view = ConfirmAction(confirm_callback=do_clear)
    await interaction.response.send_message("Are you sure you want to clear all current fronts?", view=view, ephemeral=True)


# Example usage for removetag:
@tree.command(name="removetag", description="Remove a tag from the system")
async def removetag(interaction: discord.Interaction, tag: str):
    tag = tag.strip().lower()

    if tag not in PRESET_TAGS:
        await interaction.response.send_message("Tag not found.", ephemeral=True)
        return

    async def do_remove(interaction: discord.Interaction):
        PRESET_TAGS.remove(tag)
        for m in members.values():
            if tag in m.get("tags", []):
                m["tags"].remove(tag)
        save_tags()
        save_members()
        await interaction.response.send_message(f"Tag `{tag}` removed.", ephemeral=True)

    view = ConfirmAction(confirm_callback=do_remove)
    await interaction.response.send_message(f"Are you sure you want to remove the tag `{tag}` from the system?", view=view, ephemeral=True)
# -----------------------------
# Remove member
# -----------------------------
class ConfirmRemove(discord.ui.View):
    def __init__(self, member_id):
        super().__init__(timeout=30)
        self.member_id = member_id

    @discord.ui.button(label="Confirm", style=discord.ButtonStyle.danger)
    async def confirm(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self.member_id in members:
            name = members[self.member_id]["name"]
            del members[self.member_id]
            save_members()
            await interaction.response.edit_message(content=f"Member **{name}** removed.", view=None)
        else:
            await interaction.response.edit_message(content="Member not found.", view=None)

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.edit_message(content="Action cancelled.", view=None)

@tree.command(name="removemember", description="Remove a member from the system")
@app_commands.default_permissions(administrator=True)
@app_commands.autocomplete(member_id=member_id_autocomplete)
async def removemember(interaction: discord.Interaction, member_id: str):
    if member_id not in members:
        await interaction.response.send_message("Member not found.", ephemeral=True)
        return
    await interaction.response.send_message(
        f"Are you sure you want to remove **{members[member_id]['name']}**?",
        view=ConfirmRemove(member_id),
        ephemeral=True
    )
# -----------------------------
# Multi-member removal
# -----------------------------
class MultiMemberSelect(discord.ui.Select):
    def __init__(self):
        options = [
            discord.SelectOption(label=m["name"], value=m["id"])
            for m in members.values()
        ]
        super().__init__(
            placeholder="Select members to remove...",
            min_values=1,
            max_values=len(options),
            options=options
        )

    async def callback(self, interaction: discord.Interaction):
        self.view.selected_ids = self.values
        await interaction.response.defer()

class MultiMemberView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=120)
        self.selected_ids = []
        self.add_item(MultiMemberSelect())
        self.add_item(ConfirmTags())

@tree.command(name="removemembers", description="Remove multiple members at once (with confirmation)")
async def removemembers(interaction: discord.Interaction):
    if not members:
        await interaction.response.send_message("No members exist to remove.", ephemeral=True)
        return

    view = MultiMemberView()
    await interaction.response.send_message(
        "Select the members you want to remove, then click Confirm.",
        view=view,
        ephemeral=True
    )
    await view.wait()

    if not view.selected_ids:
        await interaction.followup.send("No members were selected.", ephemeral=True)
        return

    selected_names = [members[mid]["name"] for mid in view.selected_ids]
    
    async def do_remove(interaction: discord.Interaction):
        for mid in view.selected_ids:
            del members[mid]
        save_members()
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
    def __init__(self):
        super().__init__(timeout=30)

    @discord.ui.button(label="Confirm", style=discord.ButtonStyle.danger)
    async def confirm(self, interaction: discord.Interaction, button: discord.ui.Button):
        members.clear()
        save_members()
        await interaction.response.edit_message(content="All members have been removed from the system.", view=None)

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.edit_message(content="Action cancelled.", view=None)

@tree.command(name="clearsystem", description="Remove all members from the system (dangerous!)")
async def clearsystem(interaction: discord.Interaction):
    await interaction.response.send_message(
        "Are you sure you want to remove **all members** from the system?",
        view=ConfirmClearSystem(),
        ephemeral=True
    )
# -----------------------------
# Clear all front history
# -----------------------------
@tree.command(name="clearall", description="Clear all front history for all members")
async def clearall(interaction: discord.Interaction):
    async def do_clear(interaction: discord.Interaction):
        for m in members.values():
            m["front_history"] = []
            m["fronting"] = False
            m["cofronting"] = []
            m["fronting_since"] = None
        save_members()
        await interaction.response.send_message("All front history has been cleared.", ephemeral=True)

    view = ConfirmAction(confirm_callback=do_clear)
    await interaction.response.send_message(
        "Are you sure you want to clear all front history? This cannot be undone.",
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
    global members, PRESET_TAGS

    if os.path.exists(JSON_FILE):
        with open(JSON_FILE, "r") as f:
            members.clear()
            members.update(json.load(f))
    else:
        members.clear()

    if os.path.exists(TAGS_FILE):
        with open(TAGS_FILE, "r") as f:
            PRESET_TAGS.clear()
            PRESET_TAGS.extend(json.load(f))
    else:
        PRESET_TAGS.clear()

    await interaction.response.send_message(
        f"Databases refreshed. Loaded **{len(members)}** members and **{len(PRESET_TAGS)}** tags."
    )

# -----------------------------
# Sync
# -----------------------------
@tree.command(name="synccommands", description="Force sync all commands globally")
@app_commands.default_permissions(administrator=True)
async def synccommands(interaction: discord.Interaction):
    await interaction.response.defer()
    await tree.sync()
    await interaction.followup.send("All commands synced globally!")
# -----------------------------
# Bot ready
# -----------------------------
@bot.event
async def on_ready():
    print(f"Logged in as {bot.user}")

    synced = await tree.sync()
    print(f"Synced {len(synced)} commands")

    print("Commands synced!")

bot.run(TOKEN)
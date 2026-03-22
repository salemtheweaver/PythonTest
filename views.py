import discord

from config import bot
from data import systems_data, save_systems
from helpers import get_group_settings, build_group_order_embed, get_scope_label, get_system_members


# -----------------------------
# Group order reordering UI
# -----------------------------
class GroupOrderView(discord.ui.View):
    def __init__(self, owner_id, system):
        super().__init__(timeout=180)
        self.owner_id = owner_id
        self.system = system
        settings = get_group_settings(system)
        groups = settings.get("groups", {})
        self.order = [gid for gid in settings.get("order", []) if gid in groups]
        self.focus_index = 0
        self._update_button_state()

    def _update_button_state(self):
        no_groups = len(self.order) == 0
        at_top = self.focus_index <= 0
        at_bottom = self.focus_index >= len(self.order) - 1

        self.focus_up_button.disabled = no_groups or at_top
        self.focus_down_button.disabled = no_groups or at_bottom
        self.move_up_button.disabled = no_groups or at_top
        self.move_down_button.disabled = no_groups or at_bottom
        self.save_button.disabled = no_groups

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.owner_id:
            await interaction.response.send_message("Only the command author can use this UI.", ephemeral=True)
            return False
        return True

    def current_embed(self):
        return build_group_order_embed(self.system, self.order, self.focus_index)

    async def on_timeout(self):
        self.focus_up_button.disabled = True
        self.focus_down_button.disabled = True
        self.move_up_button.disabled = True
        self.move_down_button.disabled = True
        self.save_button.disabled = True
        self.cancel_button.disabled = True
        # Push the disabled state to Discord so the UI reflects the timeout.
        if hasattr(self, "message") and self.message is not None:
            try:
                await self.message.edit(view=self)
            except (discord.HTTPException, discord.NotFound):
                pass

    @discord.ui.button(label="Focus Up", style=discord.ButtonStyle.secondary)
    async def focus_up_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self.focus_index > 0:
            self.focus_index -= 1
        self._update_button_state()
        await interaction.response.edit_message(embed=self.current_embed(), view=self)

    @discord.ui.button(label="Focus Down", style=discord.ButtonStyle.secondary)
    async def focus_down_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self.focus_index < len(self.order) - 1:
            self.focus_index += 1
        self._update_button_state()
        await interaction.response.edit_message(embed=self.current_embed(), view=self)

    @discord.ui.button(label="Move Up", style=discord.ButtonStyle.primary)
    async def move_up_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self.focus_index > 0:
            i = self.focus_index
            self.order[i - 1], self.order[i] = self.order[i], self.order[i - 1]
            self.focus_index -= 1
        self._update_button_state()
        await interaction.response.edit_message(embed=self.current_embed(), view=self)

    @discord.ui.button(label="Move Down", style=discord.ButtonStyle.primary)
    async def move_down_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self.focus_index < len(self.order) - 1:
            i = self.focus_index
            self.order[i + 1], self.order[i] = self.order[i], self.order[i + 1]
            self.focus_index += 1
        self._update_button_state()
        await interaction.response.edit_message(embed=self.current_embed(), view=self)

    @discord.ui.button(label="Save", style=discord.ButtonStyle.success)
    async def save_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        settings = get_group_settings(self.system)
        groups = settings.get("groups", {})

        # Keep any newly created groups not shown in this session appended at the end.
        seen = set(self.order)
        merged_order = [gid for gid in self.order if gid in groups]
        for gid in settings.get("order", []):
            if gid in groups and gid not in seen:
                merged_order.append(gid)
        for gid in groups.keys():
            if gid not in seen and gid not in merged_order:
                merged_order.append(gid)

        settings["order"] = merged_order
        save_systems()
        await interaction.response.edit_message(content="Group order saved.", embed=None, view=None)
        self.stop()

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.danger)
    async def cancel_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.edit_message(content="Group order edit cancelled.", embed=None, view=None)
        self.stop()


# -----------------------------
# Tag selection views
# -----------------------------
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


# -----------------------------
# Co-front UI
# -----------------------------
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


# -----------------------------
# Remove member confirmation
# -----------------------------
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


# -----------------------------
# Multi-member removal
# -----------------------------
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


# -----------------------------
# Clear all members (dangerous!)
# -----------------------------
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

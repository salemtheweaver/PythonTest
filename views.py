import discord

from config import bot
from data import systems_data, save_systems
from helpers import (
    get_group_settings, build_group_order_cv2, get_scope_label, get_system_members,
    cv2_view, cv2_simple, cv2_container, _cv2_color,
)


# -----------------------------
# Group order reordering UI (CV2 LayoutView)
# -----------------------------

class _FocusUpButton(discord.ui.Button):
    def __init__(self):
        super().__init__(label="Focus Up", style=discord.ButtonStyle.secondary)

    async def callback(self, interaction: discord.Interaction):
        view: GroupOrderView = self.view
        if view.focus_index > 0:
            view.focus_index -= 1
        view._rebuild()
        await interaction.response.edit_message(view=view)


class _FocusDownButton(discord.ui.Button):
    def __init__(self):
        super().__init__(label="Focus Down", style=discord.ButtonStyle.secondary)

    async def callback(self, interaction: discord.Interaction):
        view: GroupOrderView = self.view
        if view.focus_index < len(view.order) - 1:
            view.focus_index += 1
        view._rebuild()
        await interaction.response.edit_message(view=view)


class _MoveUpButton(discord.ui.Button):
    def __init__(self):
        super().__init__(label="Move Up", style=discord.ButtonStyle.primary)

    async def callback(self, interaction: discord.Interaction):
        view: GroupOrderView = self.view
        if view.focus_index > 0:
            i = view.focus_index
            view.order[i - 1], view.order[i] = view.order[i], view.order[i - 1]
            view.focus_index -= 1
        view._rebuild()
        await interaction.response.edit_message(view=view)


class _MoveDownButton(discord.ui.Button):
    def __init__(self):
        super().__init__(label="Move Down", style=discord.ButtonStyle.primary)

    async def callback(self, interaction: discord.Interaction):
        view: GroupOrderView = self.view
        if view.focus_index < len(view.order) - 1:
            i = view.focus_index
            view.order[i + 1], view.order[i] = view.order[i], view.order[i + 1]
            view.focus_index += 1
        view._rebuild()
        await interaction.response.edit_message(view=view)


class _SaveButton(discord.ui.Button):
    def __init__(self):
        super().__init__(label="Save", style=discord.ButtonStyle.success)

    async def callback(self, interaction: discord.Interaction):
        view: GroupOrderView = self.view
        settings = get_group_settings(view.system)
        groups = settings.get("groups", {})

        # Keep any newly created groups not shown in this session appended at the end.
        seen = set(view.order)
        merged_order = [gid for gid in view.order if gid in groups]
        for gid in settings.get("order", []):
            if gid in groups and gid not in seen:
                merged_order.append(gid)
        for gid in groups.keys():
            if gid not in seen and gid not in merged_order:
                merged_order.append(gid)

        settings["order"] = merged_order
        save_systems()
        done_view = cv2_simple("Group Order", "Group order saved.", color="00DE9B")
        await interaction.response.edit_message(view=done_view)
        view.stop()


class _CancelButton(discord.ui.Button):
    def __init__(self):
        super().__init__(label="Cancel", style=discord.ButtonStyle.danger)

    async def callback(self, interaction: discord.Interaction):
        view: GroupOrderView = self.view
        cancel_view = cv2_simple("Group Order", "Group order edit cancelled.")
        await interaction.response.edit_message(view=cancel_view)
        view.stop()


class GroupOrderView(discord.ui.LayoutView):
    def __init__(self, owner_id, system):
        super().__init__(timeout=180)
        self.owner_id = owner_id
        self.system = system
        settings = get_group_settings(system)
        groups = settings.get("groups", {})
        self.order = [gid for gid in settings.get("order", []) if gid in groups]
        self.focus_index = 0
        self._rebuild()

    def _rebuild(self):
        self.clear_items()
        # Content container from helpers
        container = build_group_order_cv2(self.system, self.order, self.focus_index)
        self.add_item(container)

        # Button state
        no_groups = len(self.order) == 0
        at_top = self.focus_index <= 0
        at_bottom = self.focus_index >= len(self.order) - 1

        focus_up = _FocusUpButton()
        focus_up.disabled = no_groups or at_top
        focus_down = _FocusDownButton()
        focus_down.disabled = no_groups or at_bottom
        move_up = _MoveUpButton()
        move_up.disabled = no_groups or at_top
        move_down = _MoveDownButton()
        move_down.disabled = no_groups or at_bottom
        save = _SaveButton()
        save.disabled = no_groups
        cancel = _CancelButton()

        row1 = discord.ui.ActionRow(focus_up, focus_down, move_up, move_down)
        row2 = discord.ui.ActionRow(save, cancel)
        self.add_item(row1)
        self.add_item(row2)

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.owner_id:
            await interaction.response.send_message("Only the command author can use this UI.", ephemeral=True)
            return False
        return True

    async def on_timeout(self):
        # Disable all buttons on timeout
        for item in self.children:
            if isinstance(item, discord.ui.ActionRow):
                for child in item.children:
                    if isinstance(child, discord.ui.Button):
                        child.disabled = True
        if hasattr(self, "message") and self.message is not None:
            try:
                await self.message.edit(view=self)
            except (discord.HTTPException, discord.NotFound):
                pass


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
        title = f"Members matching tags: {', '.join(tag_list)}"
        container = cv2_container(
            discord.ui.TextDisplay(f"### {title}\n{desc}"),
            color="00DE9B",
        )
        # Rebuild a LayoutView with the result container and the select dropdown
        result_view = discord.ui.LayoutView()
        result_view.add_item(container)
        # Re-add the tag select so users can filter again
        result_view.add_item(discord.ui.ActionRow(TagMultiSelect(
            [opt.value for opt in self.options],
            self.members_dict,
        )))
        await interaction.response.edit_message(view=result_view)


class TagMultiView(discord.ui.LayoutView):
    def __init__(self, available_tags, members_dict):
        super().__init__(timeout=None)
        # Intro container
        intro = cv2_container(
            discord.ui.TextDisplay("### Tag Browser\nSelect one or more tags from the dropdown."),
            color="00DE9B",
        )
        self.add_item(intro)
        if available_tags:
            self.add_item(discord.ui.ActionRow(TagMultiSelect(available_tags, members_dict)))


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

"""Discord UI components: buttons, selects, modals, paginators, and confirmation dialogs."""

import discord

from data import systems_data, save_systems
from helpers import (
    get_group_settings, build_group_order_cv2, get_scope_label, get_system_members,
    cv2_view, cv2_simple, cv2_container, _cv2_color,
)


# -----------------------------
# Group order reordering UI (CV2 LayoutView)
# -----------------------------

class _FocusUpButton(discord.ui.Button):
    """Button to focus up in the group order UI."""

    def __init__(self):
        super().__init__(label="Focus Up", style=discord.ButtonStyle.secondary)

    async def callback(self, interaction: discord.Interaction):
        view: GroupOrderView = self.view
        if view.focus_index > 0:
            view.focus_index -= 1
        view._rebuild()
        await interaction.response.edit_message(view=view)


class _FocusDownButton(discord.ui.Button):
    """Button to focus down in the group order UI."""

    def __init__(self):
        super().__init__(label="Focus Down", style=discord.ButtonStyle.secondary)

    async def callback(self, interaction: discord.Interaction):
        view: GroupOrderView = self.view
        if view.focus_index < len(view.order) - 1:
            view.focus_index += 1
        view._rebuild()
        await interaction.response.edit_message(view=view)


class _MoveUpButton(discord.ui.Button):
    """Button to move up in the group order UI."""

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
    """Button to move down in the group order UI."""

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
    """Save the reordered group list."""

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
    """Cancel group reordering without saving."""

    def __init__(self):
        super().__init__(label="Cancel", style=discord.ButtonStyle.danger)

    async def callback(self, interaction: discord.Interaction):
        view: GroupOrderView = self.view
        cancel_view = cv2_simple("Group Order", "Group order edit cancelled.")
        await interaction.response.edit_message(view=cancel_view)
        view.stop()


class GroupOrderView(discord.ui.LayoutView):
    """Interactive LayoutView for reordering member groups with focus/move controls. Times out after 3 minutes."""

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
    """Dropdown select for choosing member tags from available options."""

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
    """Button to confirm tag selection and close the view."""

    def __init__(self):
        super().__init__(label="Confirm", style=discord.ButtonStyle.green)

    async def callback(self, interaction: discord.Interaction):
        self.view.stop()
        await interaction.response.defer()


class TagView(discord.ui.View):
    """View combining a tag dropdown selector with a confirm button."""

    def __init__(self, available_tags, preselected=None):
        super().__init__(timeout=120)
        self.selected_tags = preselected or []
        if available_tags:
            self.add_item(TagSelect(available_tags, preselected))
        self.add_item(ConfirmTags())


class TagMultiSelect(discord.ui.Select):
    """Dropdown for filtering members by one or more tags. Rebuilds the view with matching results."""

    def __init__(self, available_tags, members_dict, side_id=None, subsystem_id=None):
        self.members_dict = members_dict
        self.side_id = side_id
        self.subsystem_id = subsystem_id
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
            side_id=self.side_id,
            subsystem_id=self.subsystem_id
        )))
        await interaction.response.edit_message(view=result_view)


class TagMultiView(discord.ui.LayoutView):
    """LayoutView wrapper for the tag browser with intro text and tag dropdown."""

    def __init__(self, available_tags, members_dict, side_id=None, subsystem_id=None):
        super().__init__(timeout=None)
        self.side_id = side_id
        self.subsystem_id = subsystem_id
        # Intro container
        intro = cv2_container(
            discord.ui.TextDisplay("### Tag Browser\nSelect one or more tags from the dropdown."),
            color="00DE9B",
        )
        self.add_item(intro)
        if available_tags:
            self.add_item(discord.ui.ActionRow(TagMultiSelect(available_tags, members_dict, side_id=side_id, subsystem_id=subsystem_id)))


# -----------------------------
# Co-front UI
# -----------------------------
class CoFrontSelect(discord.ui.Select):
    """Dropdown for selecting co-fronting members on the current page."""

    def __init__(self, parent_view):
        self.parent_view = parent_view
        self.side_id = getattr(parent_view, 'side_id', None)
        self.subsystem_id = getattr(parent_view, 'subsystem_id', None)
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
    """Paginated view for selecting co-fronting members, with Previous/Next navigation and Confirm/Cancel buttons."""

    def __init__(self, members_dict, main_member_id, side_id=None, subsystem_id=None):
        super().__init__(timeout=120)
        self.main_member_id = str(main_member_id)
        self.side_id = side_id
        self.subsystem_id = subsystem_id
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
    """Generic confirm/cancel dialog for dangerous actions. 30-second timeout."""

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
    """Confirmation dialog for removing a single member, scope-aware (main/side/subsystem)."""

    def __init__(self, member_id, system_id, side_id=None, subsystem_id=None):
        super().__init__(timeout=30)
        self.member_id = member_id
        self.system_id = system_id
        self.side_id = side_id
        self.subsystem_id = subsystem_id

    @discord.ui.button(label="Confirm", style=discord.ButtonStyle.danger)
    async def confirm(self, interaction: discord.Interaction, button: discord.ui.Button):
        from helpers import get_side_system
        system = systems_data["systems"][self.system_id]
        if self.side_id:
            side = get_side_system(system, self.side_id)
            if not side:
                await interaction.response.edit_message(content="Side system not found.", view=None)
                return
            if self.subsystem_id:
                subs = side.get("subsystems", {})
                if self.subsystem_id not in subs:
                    await interaction.response.edit_message(content="Subsystem not found in side system.", view=None)
                    return
                members_dict = subs[self.subsystem_id]["members"]
            else:
                members_dict = side.get("members", {})
        else:
            if self.subsystem_id:
                subsystems = system.get("subsystems", {})
                if self.subsystem_id not in subsystems:
                    await interaction.response.edit_message(content="Subsystem not found.", view=None)
                    return
                members_dict = subsystems[self.subsystem_id]["members"]
            else:
                members_dict = system.get("members", {})
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
    """Dropdown for selecting members to remove on the current page."""

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
    """Paginated view for bulk-selecting members to remove, with navigation and confirm/cancel."""

    def __init__(self, members_dict, side_id=None, subsystem_id=None):
        super().__init__(timeout=120)
        self.members_dict = members_dict
        self.side_id = side_id
        self.subsystem_id = subsystem_id
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
    """Confirmation dialog that removes ALL members from the entire system hierarchy."""

    def __init__(self, system):
        super().__init__(timeout=30)
        self.system = system

    @discord.ui.button(label="Confirm", style=discord.ButtonStyle.danger)
    async def confirm(self, interaction: discord.Interaction, button: discord.ui.Button):
        # Clear main system members
        self.system["members"] = {}

        # Clear all main system subsystems
        for subsystem in self.system.get("subsystems", {}).values():
            subsystem["members"] = {}

        # Clear all side system members and their subsystems
        for side in self.system.get("side_systems", {}).values():
            # Clear side system members
            side["members"] = {}
            # Clear all subsystems within this side system
            for sub in side.get("subsystems", {}).values():
                sub["members"] = {}

        save_systems()
        await interaction.response.edit_message(
            content="All members have been removed from your entire system (main, all side systems, and all subsystems).",
            view=None
        )

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.edit_message(content="Action cancelled.", view=None)

from typing import Literal

from sports_calendar.core.selection.filters import SelectionFilter
from sports_calendar.core.selection.models import Selection, SelectionItem
from sports_calendar.infra.storage.selection import SelectionStorage

from .registry import SelectionRegistry


class SelectionService:
    """
    Service layer for managing Selections, SelectionItems, and SelectionFilters.

    This class provides a high-level API for interacting with the Selection system,
    handling in-memory operations via the SelectionRegistry and persisting changes
    through SelectionStorage. All returned objects are deep copies; modifications
    must be applied using the appropriate replace or add methods to persist changes.

    Methods:
    - initialize_registry()
        Load all selections from storage into the registry.

    - get_selection(name: str) -> Selection
        Retrieve a deepcopy of a Selection by name.

    - get_all_selections() -> list[Selection]
        Retrieve deepcopies of all Selections.

    - selection_exists(name: str) -> bool
        Check if a Selection exists by name.

    - add_selection(selection: Selection)
        Add a new Selection and persist it.

    - add_empty_selection(name: str) -> Selection
        Create and add an empty Selection with the given name.

    - replace_selection(selection: Selection)
        Replace an existing Selection with a new version.

    - remove_selection(name: str)
        Remove a Selection by name.

    - clone_selection(name: str, new_name: str) -> Selection
        Clone an existing Selection under a new name.

    - get_item(selection_name: str, item_uid: str) -> SelectionItem
        Retrieve a deepcopy of a SelectionItem from a Selection.

    - add_item(selection_name: str, item: SelectionItem)
        Add a SelectionItem to a Selection and persist changes.

    - add_empty_item(selection_name: str, sport: str) -> SelectionItem
        Create and add an empty SelectionItem for a given sport.

    - replace_item(selection_name: str, item: SelectionItem)
        Replace an existing SelectionItem in a Selection.

    - remove_item(selection_name: str, item_uid: str)
        Remove a SelectionItem from a Selection by UID.

    - clone_item(selection_name: str, item_uid: str) -> SelectionItem
        Clone a SelectionItem within its Selection.

    - get_filter(selection_name: str, item_uid: str, filter_uid: str) -> SelectionFilter
        Retrieve a deepcopy of a SelectionFilter from a SelectionItem.

    - add_filter(selection_name: str, item_uid: str, filter: SelectionFilter)
        Add a SelectionFilter to a SelectionItem and persist changes.

    - add_empty_filter(selection_name: str, item_uid: str) -> SelectionFilter
        Create and add an empty SelectionFilter of a specific type.

    - replace_filter(selection_name: str, item_uid: str, filter: SelectionFilter)
        Replace an existing SelectionFilter in a SelectionItem.

    - remove_filter(selection_name: str, item_uid: str, filter_uid: str)
        Remove a SelectionFilter from a SelectionItem by UID.

    - clone_filter(selection_name: str, item_uid: str, filter_uid: str) -> SelectionFilter
        Clone a SelectionFilter within its SelectionItem.
    """

    @staticmethod
    def initialize_registry():
        SelectionRegistry.initialize(SelectionStorage.load_all())

    # Selection operations

    @staticmethod
    def get_selection(name: str) -> Selection:
        return SelectionRegistry.get_selection(name)

    @staticmethod
    def get_all_selections(
        sort_by: Literal["name", "created_at", "updated_at"] | None = None,
        order: Literal["asc", "desc"] = "asc",
    ) -> list[Selection]:
        return SelectionRegistry.get_all(sort_by=sort_by, order=order)

    @staticmethod
    def selection_exists(name: str) -> bool:
        return SelectionRegistry.exists(name)

    @staticmethod
    def add_selection(selection: Selection):
        SelectionRegistry.add(selection)

    @staticmethod
    def add_empty_selection(name: str) -> Selection:
        return SelectionRegistry.add_empty(name)

    @staticmethod
    def replace_selection(selection: Selection):
        SelectionRegistry.replace(selection)

    @staticmethod
    def remove_selection(name: str):
        SelectionRegistry.remove(name)

    @staticmethod
    def clone_selection(name: str, new_name: str) -> Selection:
        return SelectionRegistry.clone(name, new_name)

    # Item operations

    @staticmethod
    def get_item(item_uid: str) -> SelectionItem:
        return SelectionRegistry.get_item(item_uid)

    @staticmethod
    def add_item(selection_name: str, item: SelectionItem):
        selection = SelectionRegistry.get_selection(selection_name)
        selection.add_item(item)
        SelectionRegistry.replace(selection)

    @staticmethod
    def add_empty_item(selection_name: str, sport: str, name: str = '') -> SelectionItem:
        selection = SelectionRegistry.get_selection(selection_name)
        item = SelectionItem.empty(sport, name)
        selection.add_item(item)
        SelectionRegistry.replace(selection)
        return item

    @staticmethod
    def replace_item(item: SelectionItem):
        item_context = SelectionRegistry.get_item_context(item.uid)
        item_context.selection.replace_item(item)
        SelectionRegistry.replace(item_context.selection)

    @staticmethod
    def rename_item(item_uid: str, new_name: str):
        item_context = SelectionRegistry.get_item_context(item_uid)
        item_context.item.name = new_name
        item_context.selection.replace_item(item_context.item)
        SelectionRegistry.replace(item_context.selection)

    @staticmethod
    def remove_item(item_uid: str):
        item_context = SelectionRegistry.get_item_context(item_uid)
        item_context.selection.remove_item(item_uid)
        SelectionRegistry.replace(item_context.selection)

    @staticmethod
    def clone_item(item_uid: str) -> SelectionItem:
        item_context = SelectionRegistry.get_item_context(item_uid)
        cloned_item = item_context.item.clone()
        item_context.selection.add_item(cloned_item)
        SelectionRegistry.replace(item_context.selection)
        return cloned_item

    # Filter operations

    @staticmethod
    def get_filter(filter_uid: str) -> SelectionFilter:
        return SelectionRegistry.get_filter(filter_uid)

    @staticmethod
    def add_filter(item_uid: str, filter: SelectionFilter):
        item_context = SelectionRegistry.get_item_context(item_uid)
        item_context.item.add_filter(filter)
        item_context.selection.replace_item(item_context.item)
        SelectionRegistry.replace(item_context.selection)

    @staticmethod
    def add_empty_filter(item_uid: str, name: str = "") -> SelectionFilter:
        item_context = SelectionRegistry.get_item_context(item_uid)
        filter = SelectionFilter.empty(item_context.item.sport_id, name)
        item_context.item.add_filter(filter)
        item_context.selection.replace_item(item_context.item)
        SelectionRegistry.replace(item_context.selection)
        return filter

    @staticmethod
    def replace_filter(filter: SelectionFilter):
        filter_context = SelectionRegistry.get_filter_context(filter.uid)
        filter_context.item.replace_filter(filter)
        filter_context.selection.replace_item(filter_context.item)
        SelectionRegistry.replace(filter_context.selection)

    @staticmethod
    def rename_filter(filter_uid: str, new_name: str):
        filter_context = SelectionRegistry.get_filter_context(filter_uid)
        filter_context.filter.name = new_name
        filter_context.selection.replace_filter(filter_context.filter)
        SelectionRegistry.replace(filter_context.selection)

    @staticmethod
    def remove_filter(filter_uid: str):
        filter_context = SelectionRegistry.get_filter_context(filter_uid)
        filter_context.selection.remove_filter(filter_uid)
        SelectionRegistry.replace(filter_context.selection)

    @staticmethod
    def clone_filter(filter_uid: str) -> SelectionFilter:
        filter_context = SelectionRegistry.get_filter_context(filter_uid)
        cloned_filter = filter_context.filter.clone()
        filter_context.selection.add_filter(cloned_filter)
        SelectionRegistry.replace(filter_context.selection)
        return cloned_filter

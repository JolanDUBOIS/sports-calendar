from copy import deepcopy

from . import logger
from .model import Selection, SelectionItem, SelectionFilter
from .storage import SelectionStorage
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

    - add_empty_filter(selection_name: str, item_uid: str, filter_type: str) -> SelectionFilter
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
        return SelectionRegistry.get(name)

    @staticmethod
    def get_all_selections() -> list[Selection]:
        return SelectionRegistry.get_all()

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
    def get_item(selection_name: str, item_uid: str) -> SelectionItem:
        selection = SelectionRegistry.get(selection_name)
        return selection.get_item(item_uid)

    @staticmethod
    def add_item(selection_name: str, item: SelectionItem):
        selection = SelectionRegistry.get(selection_name)
        selection.add_item(item)
        SelectionRegistry.replace(selection)

    @staticmethod
    def add_empty_item(selection_name: str, sport: str) -> SelectionItem:
        selection = SelectionRegistry.get(selection_name)
        item = SelectionItem.empty(sport)
        selection.add_item(item)
        SelectionRegistry.replace(selection)
        return item

    @staticmethod
    def replace_item(selection_name: str, item: SelectionItem):
        selection = SelectionRegistry.get(selection_name)
        selection.replace_item(item)
        SelectionRegistry.replace(selection)

    @staticmethod
    def remove_item(selection_name: str, item_uid: str):
        selection = SelectionRegistry.get(selection_name)
        selection.remove_item(item_uid)
        SelectionRegistry.replace(selection)

    @staticmethod
    def clone_item(selection_name: str, item_uid: str) -> SelectionItem:
        selection = SelectionRegistry.get(selection_name)
        item = selection.get_item(item_uid)
        cloned_item = item.clone()
        selection.add_item(cloned_item)
        SelectionRegistry.replace(selection)
        return cloned_item

    # Filter operations

    @staticmethod
    def get_filter(selection_name: str, item_uid: str, filter_uid: str) -> SelectionFilter:
        selection = SelectionRegistry.get(selection_name)
        item = selection.get_item(item_uid)
        return item.get_filter(filter_uid)

    @staticmethod
    def add_filter(selection_name: str, item_uid: str, filter: SelectionFilter):
        selection = SelectionRegistry.get(selection_name)
        item = selection.get_item(item_uid)
        item.add_filter(filter)
        SelectionRegistry.replace(selection)

    @staticmethod
    def add_empty_filter(selection_name: str, item_uid: str, filter_type: str) -> SelectionFilter:
        selection = SelectionRegistry.get(selection_name)
        item = selection.get_item(item_uid)
        filter = SelectionFilter.empty(item.sport, filter_type)
        item.add_filter(filter)
        SelectionRegistry.replace(selection)
        return filter

    @staticmethod
    def replace_filter(selection_name: str, item_uid: str, filter: SelectionFilter):
        selection = SelectionRegistry.get(selection_name)
        item = selection.get_item(item_uid)
        item.replace_filter(filter)
        SelectionRegistry.replace(selection)

    @staticmethod
    def remove_filter(selection_name: str, item_uid: str, filter_uid: str):
        selection = SelectionRegistry.get(selection_name)
        item = selection.get_item(item_uid)
        item.remove_filter(filter_uid)
        SelectionRegistry.replace(selection)

    @staticmethod
    def clone_filter(selection_name: str, item_uid: str, filter_uid: str) -> SelectionFilter:
        selection = SelectionRegistry.get(selection_name)
        item = selection.get_item(item_uid)
        filter = item.get_filter(filter_uid)
        cloned_filter = filter.clone()
        item.add_filter(cloned_filter)
        SelectionRegistry.replace(selection)
        return cloned_filter

from __future__ import annotations
from uuid import uuid4
from dataclasses import dataclass, field

from . import logger
from .filters import SelectionFilter
from ..utils import validate


@dataclass
class Selection:
    name: str
    items: list[SelectionItem] = field(default_factory=list)

    def __post_init__(self):
        validate(bool(self.name), "Selection name must be a non-empty string", logger)
        validate(isinstance(self.items, list), "Selection items must be a list", logger, TypeError)
        for item in self.items:
            validate(isinstance(item, SelectionItem), "Selection items must be of type SelectionItem", logger, TypeError)

    @property
    def uid(self) -> str:
        # Compatibility alias. Do not use in new code. Identity is now based on name.
        return self.name

    @property
    def sports(self) -> list[str]:
        return list({item.sport for item in self.items})

    def _items_uids(self):
        return [item.uid for item in self.items]

    def get_item(self, item_uid: str) -> SelectionItem:
        for item in self.items:
            if item.uid == item_uid:
                return item
        logger.error(f"Selection item with uid '{item_uid}' not found in selection '{self.uid}'.")
        raise KeyError(f"Selection item with uid '{item_uid}' not found in selection '{self.uid}'.")

    def add_item(self, item: SelectionItem):
        validate(isinstance(item, SelectionItem), "Selection items must be of type SelectionItem", logger, TypeError)
        validate(item.uid not in self._items_uids(), f"Selection item with uid '{item.uid}' already exists in selection '{self.uid}'", logger, ValueError)
        self.items.append(item)
        logger.debug(f"Added item {item.uid} to selection {self.uid}")

    def replace_item(self, item: SelectionItem):
        validate(isinstance(item, SelectionItem), "Selection items must be of type SelectionItem", logger, TypeError)
        validate(item.uid in self._items_uids(), f"Selection item with uid '{item.uid}' does not exist in selection '{self.uid}'", logger, KeyError)
        self.items = [i for i in self.items if i.uid != item.uid]
        self.items.append(item)
        logger.debug(f"Replaced item {item.uid} in selection {self.uid}")

    def remove_item(self, item_uid: str):
        if not item_uid in self._items_uids():
            logger.error(f"Item {item_uid} not found in selection {self.uid}")
            raise KeyError(f"Item {item_uid} not found in selection {self.uid}")
        self.items = [item for item in self.items if item.uid != item_uid]
        logger.debug(f"Removed item {item_uid} from selection {self.uid}")
    
    def clone(self, new_name: str) -> Selection:
        cloned_items = [item.clone() for item in self.items]
        return Selection(
            name=new_name,
            items=cloned_items
        )

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "items": [item.to_dict() for item in self.items]
        }

    @classmethod
    def from_dict(cls, data: dict) -> Selection:
        data = dict(data)
        items_data = data.pop("items", [])
        items = [SelectionItem.from_dict(item_data) for item_data in items_data]
        return cls(
            items=items,
            **data
        )

    @classmethod
    def empty(cls, name: str) -> Selection:
        return cls(name=name)


@dataclass
class SelectionItem:
    sport: str
    uid: str = field(default_factory=lambda: str(uuid4())[:8], kw_only=True)
    filters: list[SelectionFilter] = field(default_factory=list)

    def __post_init__(self):
        validate(bool(self.sport), "SelectionItem sport must be a non-empty string", logger)
        validate(isinstance(self.filters, list), "SelectionItem filters must be a list", logger, TypeError)
        for f in self.filters:
            validate(isinstance(f, SelectionFilter), "SelectionItem filters must be of type SelectionFilter", logger, TypeError)

    def _filters_uids(self):
        return [f.uid for f in self.filters]

    def get_filter(self, filter_uid: str) -> SelectionFilter:
        for f in self.filters:
            if f.uid == filter_uid:
                return f
        logger.error(f"Selection filter with uid '{filter_uid}' not found in selection item '{self.uid}'.")
        raise KeyError(f"Selection filter with uid '{filter_uid}' not found in selection item '{self.uid}'.")

    def add_filter(self, selection_filter: SelectionFilter):
        validate(isinstance(selection_filter, SelectionFilter), "SelectionItem filters must be of type SelectionFilter", logger, TypeError)
        validate(selection_filter.sport == self.sport, "SelectionFilter sport must match SelectionItem sport", logger, ValueError)
        validate(selection_filter.uid not in self._filters_uids(), f"Selection filter with uid '{selection_filter.uid}' already exists in selection item '{self.uid}'", logger, ValueError)
        self.filters.append(selection_filter)
        logger.debug(f"Added filter {selection_filter.uid} to selection item {self.uid}")

    def replace_filter(self, selection_filter: SelectionFilter):
        validate(isinstance(selection_filter, SelectionFilter), "SelectionItem filters must be of type SelectionFilter", logger, TypeError)
        validate(selection_filter.sport == self.sport, "SelectionFilter sport must match SelectionItem sport", logger, ValueError)
        validate(selection_filter.uid in self._filters_uids(), f"Selection filter with uid '{selection_filter.uid}' does not exist in selection item '{self.uid}'", logger, KeyError)
        self.filters = [f for f in self.filters if f.uid != selection_filter.uid]
        self.filters.append(selection_filter)
        logger.debug(f"Replaced filter {selection_filter.uid} in selection item {self.uid}")

    def remove_filter(self, filter_uid: str):
        if not filter_uid in self._filters_uids():
            logger.error(f"Filter {filter_uid} not found in selection item {self.uid}")
            raise KeyError(f"Filter {filter_uid} not found in selection item {self.uid}")
        self.filters = [f for f in self.filters if f.uid != filter_uid]
        logger.debug(f"Removed filter {filter_uid} from selection item {self.uid}")
    
    def clone(self) -> SelectionItem:
        cloned_filters = [f.clone() for f in self.filters]
        return SelectionItem(
            sport=self.sport,
            filters=cloned_filters
        )

    def to_dict(self) -> dict:
        return {
            "sport": self.sport,
            "uid": self.uid,
            "filters": [f.to_dict() for f in self.filters]
        }

    @classmethod
    def from_dict(cls, data: dict) -> SelectionItem:
        data = dict(data)
        filters_data = data.pop("filters", [])
        filters = [SelectionFilter.from_dict(sport=data.get("sport"), data=filter_data) for filter_data in filters_data]
        return cls(
            filters=filters,
            **data
        )

    @classmethod
    def empty(cls, sport: str) -> SelectionItem:
        return cls(sport=sport)

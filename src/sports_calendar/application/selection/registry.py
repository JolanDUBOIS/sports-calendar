import contextlib
import logging
from collections.abc import Iterable
from copy import deepcopy
from dataclasses import dataclass
from typing import Literal

from sports_calendar.core.selection.models import (
    Selection,
    SelectionFilter,
    SelectionItem,
)
from sports_calendar.core.utils import validate
from sports_calendar.infra.storage import SelectionStorage

logger = logging.getLogger(__name__)


@dataclass
class ItemContext:
    selection: Selection
    item: SelectionItem

@dataclass
class FilterContext:
    selection: Selection
    item: SelectionItem
    filter: SelectionFilter


class SelectionRegistry:
    _selections: list[Selection] = []
    _initialized: bool = False

    @classmethod
    def initialize(cls, selections: Iterable[Selection]):
        validate(not cls._initialized, "SelectionRegistry already initialized", logger)
        cls._selections = []
        for sel in selections:
            validate(not cls.exists(sel.name), f"Duplicate name {sel.name}", logger, KeyError)
            cls._selections.append(sel)
        cls._initialized = True

    @classmethod
    def get_selection(cls, name: str) -> Selection:
        for sel in cls._selections:
            if sel.name == name:
                return deepcopy(sel)
        logger.error(f"Selection '{name}' not found")
        raise KeyError(f"Selection '{name}' not found")

    @classmethod
    def get_item(cls, item_uid: str) -> SelectionItem:
        for sel in cls._selections:
            with contextlib.suppress(KeyError):
                return deepcopy(sel.get_item(item_uid))
        logger.error(f"No item found for uid '{item_uid}'")
        raise KeyError(f"No item found for uid '{item_uid}'")

    @classmethod
    def get_item_context(cls, item_uid: str) -> ItemContext:
        for sel in cls._selections:
            with contextlib.suppress(KeyError):
                return deepcopy(ItemContext(selection=sel, item=sel.get_item(item_uid)))
        logger.error(f"No item found for uid '{item_uid}'")
        raise KeyError(f"No item found for uid '{item_uid}'")

    @classmethod
    def get_filter(cls, filter_uid: str) -> SelectionFilter:
        for sel in cls._selections:
            with contextlib.suppress(KeyError):
                return deepcopy(sel.get_filter(filter_uid))
        logger.error(f"No filter found for uid '{filter_uid}'")
        raise KeyError(f"No filter found for uid '{filter_uid}'")

    @classmethod
    def get_filter_context(cls, filter_uid: str) -> FilterContext:
        for sel in cls._selections:
            for item in sel.items:
                with contextlib.suppress(KeyError):
                    return deepcopy(FilterContext(
                        selection=sel,
                        item=item,
                        filter=item.get_filter(filter_uid)
                    ))
        logger.error(f"No filter found for uid '{filter_uid}'")
        raise KeyError(f"No filter found for uid '{filter_uid}'")

    @classmethod
    def get_all(
        cls,
        sort_by: Literal["name", "created_at", "updated_at"] | None = None,
        order: Literal["asc", "desc"] = "asc",
    ) -> list[Selection]:
        selections = [deepcopy(selection) for selection in cls._selections]

        if sort_by is None:
            return selections

        validate(sort_by in {"name", "created_at", "updated_at"}, f"Unsupported sort field '{sort_by}'", logger, ValueError)
        validate(order in {"asc", "desc"}, f"Unsupported sort order '{order}'", logger, ValueError)
        reverse = order == "desc"
        return sorted(selections, key=lambda selection: getattr(selection, sort_by), reverse=reverse)

    @classmethod
    def exists(cls, name: str) -> bool:
        return any(sel.name == name for sel in cls._selections)

    @classmethod
    def add(cls, selection: Selection):
        validate(not cls.exists(selection.name), "Duplicate name", logger, KeyError)
        SelectionStorage.save(selection, mode="new")
        cls._selections.append(deepcopy(selection))

    @classmethod
    def add_empty(cls, name: str) -> Selection:
        validate(not cls.exists(name), "Duplicate name", logger, KeyError)
        selection = Selection.empty(name)
        SelectionStorage.save(selection, mode="new")
        cls._selections.append(deepcopy(selection))
        return selection

    @classmethod
    def replace(cls, selection: Selection):
        validate(cls.exists(selection.name), "Selection not found", logger, KeyError)
        SelectionStorage.save(selection, mode="existing")
        cls._selections = [sel for sel in cls._selections if sel.name != selection.name]
        cls._selections.append(deepcopy(selection))

    @classmethod
    def remove(cls, name: str):
        validate(cls.exists(name), "Selection not found", logger, KeyError)
        selection = cls.get_selection(name)
        SelectionStorage.delete(selection.name)
        cls._selections = [sel for sel in cls._selections if sel.name != name]

    @classmethod
    def clone(cls, name: str, new_name: str) -> Selection:
        validate(cls.exists(name), "Selection not found", logger, KeyError)
        validate(not cls.exists(new_name), "Duplicate name", logger, KeyError)
        original = cls.get_selection(name)
        cloned = original.clone(new_name)
        cls.add(cloned)
        return cloned

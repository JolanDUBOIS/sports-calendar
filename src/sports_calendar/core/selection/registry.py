from typing import Iterable

from . import logger
from .specs import SelectionSpec
from .storage import SelectionStorage
from ..utils import validate


class SelectionRegistry:
    _selections: dict[str, SelectionSpec] = {}
    _initialized: bool = False

    @classmethod
    def initialize(cls, selections: Iterable[SelectionSpec]):
        validate(not cls._initialized, "SelectionRegistry already initialized", logger)
        cls._selections = {}
        for sel in selections:
            validate(sel.uid not in cls._selections, f"Duplicate uid {sel.uid}", logger)
            cls._assert_unique_name(sel.name)
            cls._selections[sel.uid] = sel
        cls._initialized = True

    @classmethod
    def get(cls, uid: str) -> SelectionSpec:
        validate(uid in cls._selections, f"Selection '{uid}' not found", logger, KeyError)
        return cls._selections[uid]

    @classmethod
    def get_by_name(cls, name: str) -> SelectionSpec:
        for sel in cls._selections.values():
            if sel.name == name:
                return sel
        raise KeyError(f"Selection '{name}' not found")

    @classmethod
    def get_all(cls) -> dict[str, SelectionSpec]:
        return cls._selections.copy()

    @classmethod
    def add(cls, selection: SelectionSpec):
        validate(selection.uid not in cls._selections, "Duplicate uid", logger)
        cls._assert_unique_name(selection.name)

        SelectionStorage.save(selection, mode="new")
        cls._selections[selection.uid] = selection

    @classmethod
    def replace(cls, selection: SelectionSpec):
        validate(selection.uid in cls._selections, "Selection not found", logger)
        cls._assert_unique_name(selection.name, ignore_uid=selection.uid)

        SelectionStorage.save(selection, mode="existing")
        cls._selections[selection.uid] = selection

    @classmethod
    def remove(cls, uid: str):
        validate(uid in cls._selections, "Selection not found", logger)

        selection = cls._selections[uid]

        SelectionStorage.delete(selection)
        del cls._selections[uid]

    @classmethod
    def _assert_unique_name(cls, name: str, *, ignore_uid: str | None = None):
        for uid, sel in cls._selections.items():
            validate(
                sel.name != name or uid == ignore_uid,
                f"Duplicate selection name '{name}'",
                logger
            )
from typing import Iterable
from copy import deepcopy

from . import logger
from .model import Selection
from .storage import SelectionStorage
from ..utils import validate


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
    def get(cls, name: str) -> Selection:
        for sel in cls._selections:
            if sel.name == name:
                return deepcopy(sel)
        logger.error(f"Selection '{name}' not found")
        raise KeyError(f"Selection '{name}' not found")

    @classmethod
    def get_all(cls) -> list[Selection]:
        return deepcopy(cls._selections)

    @classmethod
    def exists(cls, name: str) -> bool:
        for sel in cls._selections:
            if sel.name == name:
                return True
        return False

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
        selection = cls.get(name)
        SelectionStorage.delete(selection.name)
        cls._selections = [sel for sel in cls._selections if sel.name != name]

    @classmethod
    def clone(cls, name: str, new_name: str) -> Selection:
        validate(cls.exists(name), "Selection not found", logger, KeyError)
        validate(not cls.exists(new_name), "Duplicate name", logger, KeyError)
        original = cls.get(name)
        cloned = original.clone(new_name)
        cls.add(cloned)
        return cloned

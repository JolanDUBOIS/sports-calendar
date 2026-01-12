from . import logger
from .specs import SelectionSpec
from .factory import SelectionSpecFactory
from ..utils import validate
from sports_calendar.core import load_yml, Paths


# TODO - Lazy load !!
class SelectionManager:
    """ Load and provide access to SelectionSpec objects from the selections folder. """
    _selections: dict[str, SelectionSpec] = {}  # key = selection.uid
    _loaded: bool = False

    @classmethod
    def load_all(cls):
        validate(
            Paths.SELECTIONS_FOLDER.exists(),
            f"Selections directory does not exist: {Paths.SELECTIONS_FOLDER}",
            logger,
            FileNotFoundError
        )

        for file in Paths.SELECTIONS_FOLDER.glob("*.yml"):
            try:
                data = load_yml(file)
                sel = SelectionSpecFactory.from_dict(data)
                validate(
                    sel.name not in cls._selections,
                    f"Duplicate selection name '{sel.name}' found in {Paths.SELECTIONS_FOLDER}",
                    logger
                )
                cls._selections[sel.uid] = sel
                logger.debug(f"Loaded selection: {sel.name}")
            except Exception:
                logger.exception(f"Failed to load selection from {file}")

        cls._loaded = True

    @classmethod
    def get(cls, id: str) -> SelectionSpec:
        if not cls._loaded:
            cls.load_all()
        validate(
            id in cls._selections,
            f"Selection with id '{id}' not found.",
            logger,
            KeyError
        )
        return cls._selections[id]

    @classmethod
    def get_by_name(cls, name: str) -> SelectionSpec:
        if not cls._loaded:
            cls.load_all()
        for sel in cls._selections.values():
            if sel.name == name:
                return sel
        logger.error(f"Selection with name '{name}' not found.")
        raise KeyError(f"Selection with name '{name}' not found.")

    @classmethod
    def get_all(cls) -> dict[str, SelectionSpec]:
        if not cls._loaded:
            cls.load_all()
        return cls._selections.copy()

    @classmethod
    def add(cls, selection: SelectionSpec):
        validate(
            isinstance(selection, SelectionSpec),
            "selection must be an instance of SelectionSpec",
            logger,
            TypeError
        )
        cls._selections[selection.uid] = selection
        logger.info(f"Added selection: {selection.name} (uid={selection.uid})")

    @classmethod
    def remove(cls, uid: str):
        validate(
            uid in cls._selections,
            f"Selection with uid '{uid}' not found.",
            logger,
            KeyError
        )
        del cls._selections[uid]
        logger.info(f"Removed selection: {uid}")

    @classmethod
    def clone(cls, uid: str, new_name: str) -> SelectionSpec:
        """ Clone an existing selection with a new name. """
        original = cls.get(uid)
        cloned = original.clone(new_name=new_name)
        cls.add(cloned)
        return cloned

    @classmethod
    def empty(cls, name: str) -> SelectionSpec:
        """ Create and return an empty selection with the given name. """
        new_selection = SelectionSpec.empty(name=name)
        cls.add(new_selection)
        return new_selection

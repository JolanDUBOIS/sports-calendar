from . import logger
from .specs import SelectionSpec
from .factory import SelectionSpecFactory
from ..utils import validate
from sports_calendar.core import load_yml, Paths


class SelectionManager:
    """ Load and provide access to SelectionSpec objects from the selections folder. """

    def __init__(self):
        self._selections: dict[str, SelectionSpec] = {}
        self.load_all()

    def load_all(self):
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
                self._selections[sel.name] = sel
                logger.debug(f"Loaded selection: {sel.name}")
            except Exception:
                logger.exception(f"Failed to load selection from {file}")

    def get(self, name: str) -> SelectionSpec:
        validate(
            name in self._selections,
            f"Selection '{name}' not found.",
            logger,
            KeyError
        )
        return self._selections[name]

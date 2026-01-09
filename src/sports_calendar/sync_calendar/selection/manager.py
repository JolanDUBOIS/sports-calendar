from . import logger
from .selection import Selection
from sports_calendar.core import load_yml, Paths


class SelectionManager:

    def __init__(self):
        self.selections = {}
        self.load_selections()

    def load_selections(self): # TODO - Don't load all selections, only the ones that are needed
        """ Load all selections from the selections directory. """
        if not Paths.SELECTIONS_FOLDER.exists():
            logger.error(f"Selections directory does not exist: {Paths.SELECTIONS_FOLDER}")
            raise FileNotFoundError(f"Selections directory does not exist: {Paths.SELECTIONS_FOLDER}")
        for selection_file in Paths.SELECTIONS_FOLDER.glob("*.yml"):
            try:
                _selection = Selection.from_dict(load_yml(selection_file))
                self.selections[_selection.name] = _selection
                logger.debug(f"Loaded selection: {_selection.name}.")
            except Exception as e:
                logger.error(f"Failed to load selection from {selection_file}: {e}")

    def get_selection(self, name: str) -> Selection:
        """ Get a selection by its name. """
        if name not in self.selections:
            logger.error(f"Selection '{name}' not found.")
            raise KeyError(f"Selection '{name}' not found.")
        return self.selections[name]

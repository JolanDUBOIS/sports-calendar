from src import ROOT_PATH

from . import logger
from .selection import Selection
from src.config import load_yml


SELECTIONS_DIR_PATH = ROOT_PATH / "selections"

class SelectionManager:

    def __init__(self):
        self.selections = {}
        self.load_selections()

    def load_selections(self):
        """ Load all selections from the selections directory. """
        if not SELECTIONS_DIR_PATH.exists():
            logger.error(f"Selections directory does not exist: {SELECTIONS_DIR_PATH}")
            raise FileNotFoundError(f"Selections directory does not exist: {SELECTIONS_DIR_PATH}")
        for selection_file in SELECTIONS_DIR_PATH.glob("*.yml"):
            try:
                _selection = Selection.from_dict(load_yml(selection_file))
                self.selections[_selection.name] = _selection
            except Exception as e:
                logger.error(f"Failed to load selection from {selection_file}: {e}")
                continue

    def get_selection(self, name: str) -> Selection:
        """ Get a selection by its name. """
        if name not in self.selections:
            logger.error(f"Selection '{name}' not found.")
            raise KeyError(f"Selection '{name}' not found.")
        return self.selections[name]

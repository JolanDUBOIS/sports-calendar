import yaml

from . import logger
from .specs import SelectionSpec
from ..utils import validate
from sports_calendar.core import load_yml, Paths


class SelectionStorage:

    @staticmethod
    def load_all() -> list[SelectionSpec]:
        selections = []
        for file in Paths.SELECTIONS_FOLDER.glob("*.yml"):
            data = load_yml(file)
            selections.append(SelectionSpec.from_dict(data))
        return selections

    @staticmethod
    def save(selection: SelectionSpec, mode: str = "any"):
        """ Save selection to disk. 
        mode: 
            'any' - save regardless of existing file
            'new' - only save if file does not exist else raise error
            'existing' - only save if file exists else raise error
        """
        path = Paths.SELECTIONS_FOLDER / f"{selection.name}.yml"
        if mode == "new":
            validate(not path.exists(), f"Selection file already exists: {path}", logger, FileExistsError)
        elif mode == "existing":
            validate(path.exists(), f"Selection file does not exist: {path}", logger, FileNotFoundError)
        elif mode != "any":
            logger.error(f"Invalid save mode: {mode}")
            raise ValueError(f"Invalid save mode: {mode}")
        with open(path, "w") as f:
            yaml.safe_dump(selection.to_dict(), f)

    @staticmethod
    def delete(selection: SelectionSpec):
        """ Delete selection file from disk. """
        path = Paths.SELECTIONS_FOLDER / f"{selection.name}.yml"
        validate(path.exists(), f"Selection file does not exist: {path}", logger, FileNotFoundError)
        if path.exists():
            path.unlink()

# Issues:
# - Can't have a file name different than selection name
# - When using this in the backend, can't change the files manually without breaking everything (this issue is linked to the registry as well)
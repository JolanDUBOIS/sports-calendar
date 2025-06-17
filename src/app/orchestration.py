from __future__ import annotations
from pathlib import Path
from dataclasses import dataclass, field

import yaml

from . import logger
from .selection import Selection
from .. import ROOT_PATH
from .. config import SecretsManager


@dataclass
class SelectionParams:
    date_from: str | None = None
    date_to: str | None = None

@dataclass
class DevConfig:
    selections_params: dict[str, SelectionParams] = field(default_factory=dict)

    def get(self, key: str) -> SelectionParams | None:
        """ Get selection parameters by key. """
        return self.selections_params.get(key)

    @classmethod
    def from_dict(cls, data: dict) -> DevConfig:
        """ Create a DevConfig instance from a dictionary. """
        selections_params = {
            name: SelectionParams(**params) for name, params in data.get("selections_params", {}).items()
        }
        return cls(selections_params=selections_params)

    @classmethod
    def from_yml(cls, path: str | Path) -> DevConfig:
        """ Load a DevConfig from a YAML file. """
        path = Path(path)
        data = load_yml(path)
        if not isinstance(data, dict):
            logger.error(f"Invalid DevConfig data format in {path}. Expected a dictionary.")
            raise ValueError(f"Invalid DevConfig data format in {path}. Expected a dictionary.")
        return cls.from_dict(data)

class AppConfig:
    """ TODO """

    repository_mapping = {
        "test": ROOT_PATH / "data" / "repository_test",
        "prod": ROOT_PATH / "data" / "repository"
    }

    def __init__(self, repo: str = "prod", selections_path: Path | None = None):
        self.selections_path = selections_path or (ROOT_PATH / "config" / "app" / "selections")
        self.dev_config_path = ROOT_PATH / "config" / "app" / "dev_config.yml"
        self.repo_path = self.repository_mapping.get(repo)

        self.selections = self._load_selections()
        self.dev_config = DevConfig.from_yml(self.dev_config_path)
        self.gcal_ids: dict[str, str] = SecretsManager.get("gcal_ids", {})

    def get_selection(self, key: str) -> Selection:
        """ Get a selection by its name. """
        for selection in self.selections:
            if selection.name == key:
                return selection
        logger.error(f"Selection {key} not found.")
        raise ValueError(f"Selection {key} not found.")

    def get_selection_params(self, key: str) -> SelectionParams | None:
        """ Get selection parameters for a given key. """
        return self.dev_config.get(key)

    def get_gcal_id(self, key: str) -> str | None:
        """ Get Google Calendar ID for a given key (it should be a selection.name). """
        return self.gcal_ids.get(key)

    def _load_selections(self) -> list[Selection]:
        """ Load all selections from the configured selections path. """
        if not self.selections_path.exists():
            logger.error(f"Selections path {self.selections_path} does not exist.")
            raise FileNotFoundError(f"Selections path {self.selections_path} does not exist.")

        selections = []
        for file in self.selections_path.glob("*.yml"):
            try:
                selection = self._load_selection(file)
                selections.append(selection)
            except Exception as e:
                logger.error(f"Failed to load selection from {file}: {e}")

        return selections        

    @staticmethod
    def _load_selection(path: str | Path) -> Selection:
        """ Load a selection from a YAML file. """
        path = Path(path)
        data = load_yml(path)

        if not isinstance(data, dict):
            logger.error(f"Invalid selection data format in {path}. Expected a dictionary.")
            raise ValueError(f"Invalid selection data format in {path}. Expected a dictionary.")

        return Selection.from_dict(data)

def load_yml(path: str | Path) -> dict | list | None:
    """ Load a YAML file and return its content as a dictionary or list. """
    path = Path(path)
    if not path.exists():
        logger.error(f"YAML file {path} does not exist.")
        raise FileNotFoundError(f"YAML file {path} does not exist.")

    with open(path, 'r') as file:
        try:
            return yaml.safe_load(file)
        except yaml.YAMLError as e:
            logger.error(f"Error loading YAML file {path}: {e}")
            raise ValueError(f"Error loading YAML file {path}: {e}")

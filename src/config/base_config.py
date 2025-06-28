from dataclasses import dataclass
from pathlib import Path

from . import logger, CONFIG_DIR_PATH
from .loader import load_yml
from src import ROOT_PATH


@dataclass
class Repository:
    name: str
    path: Path

class BaseConfig:
    """ Base configuration class for the whole project. """

    def __init__(self):
        self.base_config_path = CONFIG_DIR_PATH / "base.yml"
        self._config = load_yml(self.base_config_path)
        self._repositories: dict[str, Repository] = None
        self._active_repo: Repository | None = None

    @property
    def repositories(self) -> dict[str, Repository]:
        """ Get all repositories from the base configuration. """
        if self._repositories is None:
            self._repositories = {}
            repos: dict[str, dict] = self._config.get("repositories", {})
            for key, value in repos.items():
                path = ROOT_PATH / value.get("path")
                self._repositories[key] = Repository(name=key, path=path)
        return self._repositories

    def get_repo(self, key: str) -> Repository:
        """ Get the repository configuration for a given key. """
        if key not in self.repositories:
            logger.error(f"Repository '{key}' not found in base configuration.")
            raise KeyError(f"Repository '{key}' not found in base configuration.")
        return self.repositories[key]

    def set_active_repo(self, key: str) -> None:
        """ Set the active repository for the current session. """
        logger.info(f"Setting active repository to '{key}'")
        self._active_repo = self.get_repo(key)

    @property
    def active_repo(self) -> Repository:
        """ Return the active repository. Raises if not set. """
        if self._active_repo is None:
            logger.error("No active repository set. Did you forget to call set_active_repo(key)?")
            raise RuntimeError("No active repository set. Did you forget to call set_active_repo(key)?")
        return self._active_repo

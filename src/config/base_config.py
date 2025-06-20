import shutil
from pathlib import Path
from datetime import datetime

from . import logger, CONFIG_DIR_PATH
from .loader import load_yml
from src import ROOT_PATH
from src.config import DataStage
from src.file_io import FileHandlerFactory, BaseFileHandler


class StageManager:
    """ TODO - Should only be created by RepositoryManager. """

    def __init__(self, stage: DataStage, repo_path: Path):
        """ TODO """
        self.stage = stage
        self.name = self.stage.name.lower()
        self.path = Path(repo_path) / self.name

        if not self.path.exists():
            logger.warning(f"Stage directory {self.name} does not exist. Creating it.")
            self.path.mkdir(parents=True, exist_ok=True)

    def get_files(self) -> list[Path]:
        """ Get all files in the stage directory. """
        return list(self.path.glob("*"))

    def get_handlers(self) -> list[BaseFileHandler]:
        """ Get all file handlers for the stage. """
        return [FileHandlerFactory.create_file_handler(file_path) for file_path in self.get_files()]

    def get_handler(self, filename: str) -> BaseFileHandler | None:
        """ Get a specific file handler by filename. """
        for handler in self.get_handlers():
            if handler.name == filename:
                return handler
        logger.warning(f"File handler for {filename} not found in stage {self.name}.")
        return None

    def cleanup(self, cutoff: str) -> None:
        """ TODO - Remove files older than the cutoff date. """
        logger.info(f"Cleaning up stage {self.name} for files older than {cutoff}.")
        handlers = self.get_handlers()
        for handler in handlers:
            handler.cleanup(cutoff)

    def reset(self, filename: str | None = None) -> None:
        """ TODO - Should only be called when reset has been confirmed at CLI. """
        logger.info(f"Resetting stage {self.name} for file {filename if filename else 'all files'}.")
        if filename is None:
            handlers = self.get_handlers()
        else:
            handler = self.get_handler(filename)
            if handler is None:
                return
            handlers = [handler]
        for handler in handlers:
            handler.purge()


class RepositoryManager:
    """ TODO """

    def __init__(self, name: str, path: Path | str):
        """ TODO """
        self.name = name
        self.path = Path(path)
        self.stages = {stage.name.lower(): StageManager(stage, self.path) for stage in DataStage.instances()}

    def get_stage(self, stage: DataStage) -> StageManager:
        """ Get the stage manager for a given stage. """
        try:
            return self.stages[stage.name.lower()]
        except KeyError:
            logger.error(f"No such stage '{stage}' in repository '{self.name}'.")
            raise ValueError(f"No such stage '{stage}' in repository '{self.name}'.")

    def get_stages(self) -> list[StageManager]:
        """ Get all stage managers in the repository. """
        return list(self.stages.values())

    def backup(self) -> Path:
        """ Backup all files and subfolders in the repository to the specified path. """
        ts = datetime.now().strftime("%Y%m%d-%H%M%S")
        backup_path = self.path.parent / f"{self.name}-backup-{ts}"
        shutil.copytree(self.path, backup_path, dirs_exist_ok=True)
        logger.info(f"Backup created at {backup_path}")
        return backup_path


class BaseConfig:
    """ Base configuration class for the whole project. """

    def __init__(self):
        self.base_config_path = CONFIG_DIR_PATH / "base.yml"
        self._config = load_yml(self.base_config_path)
        self._repositories: dict[str, RepositoryManager] = None
        self._active_repo: RepositoryManager | None = None

    @property
    def repositories(self) -> dict[str, RepositoryManager]:
        """ Get all repositories from the base configuration. """
        if self._repositories is None:
            self._repositories = {}
            repos: dict[str, dict] = self._config.get("repositories", {})
            for key, value in repos.items():
                path = ROOT_PATH / value.get("path")
                self._repositories[key] = RepositoryManager(name=key, path=path)
        return self._repositories

    def get_repo(self, key: str) -> RepositoryManager:
        """ Get the repository configuration for a given key. """
        if key not in self.repositories:
            logger.error(f"Repository '{key}' not found in base configuration.")
            raise KeyError(f"Repository '{key}' not found in base configuration.")
        return self.repositories[key]

    def set_active_repo(self, key: str) -> None:
        """ Set the active repository for the current session. """
        logger.info(f"Setting active repository to '{key}'.")
        self._active_repo = self.get_repo(key)

    @property
    def active_repo(self) -> RepositoryManager:
        """ Return the active repository. Raises if not set. """
        if self._active_repo is None:
            logger.error("No active repository set. Did you forget to call set_active_repo(key)?")
            raise RuntimeError("No active repository set. Did you forget to call set_active_repo(key)?")
        return self._active_repo

import shutil
from pathlib import Path

from . import logger
from .pipeline_stages import DataStage
from src.file_io import FileHandlerFactory, BaseFileHandler

REPO_MAPPING_TEMP = {
    "prod": {"path": "data/repository"},
    "test": {"path": "data/repository-test"},
    "dev": {"path": "data/repository-dev"}
}


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
        return [FileHandlerFactory.create_handler(file_path) for file_path in self.get_files()]

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
        return self.stages.get(stage.name.lower())

    def backup(self) -> Path:
        """ Backup all files and subfolders in the repository to the specified path. """
        backup_path = self.path.parent / f"{self.name}-backup"
        shutil.copytree(self.path, backup_path, dirs_exist_ok=True)
        logger.info(f"Backup created at {backup_path}")
        return backup_path

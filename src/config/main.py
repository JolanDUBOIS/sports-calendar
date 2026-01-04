from abc import ABC
from pathlib import Path

import yaml

from . import logger, CONFIG_DIR_PATH
from .pipeline import PipelineConfig
from .constants import ENV, DEFAULT_ENV


class Config:
    def __init__(
        self,
        repository: Path | str | None = None,
        environment: str | None = None
    ):
        self._pipeline: PipelineConfig | None = None
        self.load_base_config()
        self.set_repo(repository or self._data.get("repository"))
        self.set_environment(environment or self._data.get("environment", DEFAULT_ENV))

    @property
    def pipeline(self) -> PipelineConfig:
        if self._pipeline is None:
            self._pipeline = PipelineConfig(self.repository, self.environment)
        return self._pipeline

    def load_base_config(self) -> None:
        self.base_config_path = CONFIG_DIR_PATH / "base.yml"
        if not self.base_config_path.exists():
            logger.error(f"Base config file does not exist: {self.base_config_path}")
            raise FileNotFoundError(f"Base config file does not exist: {self.base_config_path}")
        self._data = self._load_yml(self.base_config_path)

    def set_repo(self, repo: Path | str) -> None:
        """ Set the repository path in the base configuration and reset pipeline. """
        logger.debug(f"Setting repository path to: {repo}")
        repo_path = Path(repo)
        if not repo_path.exists():
            logger.error(f"Repository path does not exist: {repo_path}")
            raise FileNotFoundError(f"Repository path does not exist: {repo_path}")
        self.repository = repo_path
        self._pipeline = None  # Reset pipeline to force re-initialization with new repo
        logger.info(f"Repository path set to: {self.repository}")

    def set_environment(self, environment: str) -> None:
        """ Set the active environment in the base configuration and reset pipeline. """
        logger.debug(f"Setting environment to: {environment}")
        if environment not in ENV:
            logger.error(f"Invalid environment '{environment}'. Must be one of: {ENV}")
            raise ValueError(f"Invalid environment '{environment}'. Must be one of: {ENV}")
        self.environment = environment
        self._pipeline = None  # Reset pipeline to force re-initialization with new environment
        logger.info(f"Environment set to: {self.environment}")

    @staticmethod
    def _load_yml(path: Path) -> dict:
        with open(path, "r") as f:
            _data = yaml.safe_load(f) or {}
        return _data

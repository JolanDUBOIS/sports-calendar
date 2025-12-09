from abc import ABC
from pathlib import Path

import yaml

from . import logger, CONFIG_DIR_PATH
from .pipeline import PipelineConfig
from .constants import ENV, DEFAULT_ENV


class BaseConfig(ABC):
    """ TODO """

    def __init__(self, repository: Path | str | None = None, environment: str | None = None):
        self.base_config_path = CONFIG_DIR_PATH / "base.yml"
        if not self.base_config_path.exists():
            logger.error(f"Base config file does not exist: {self.base_config_path}")
            raise FileNotFoundError(f"Base config file does not exist: {self.base_config_path}")
        
        self._load_yml()

        self.repository = repository or self._data.get("repository")
        self.environment = environment or self._data.get("environment", DEFAULT_ENV)

        if not isinstance(self.repository, Path):
            self.repository = Path(self.repository)
        if not self.repository.exists():
            logger.error(f"Repository path does not exist: {self.repository}")
            raise FileNotFoundError(f"Repository path does not exist: {self.repository}")

        if not isinstance(self.environment, str) or self.environment not in ENV:
            logger.error(f"Invalid environment '{self.environment}'. Must be one of: {', '.join(ENV)}")
            raise ValueError(f"Invalid environment '{self.environment}'. Must be one of: {', '.join(ENV)}")

    def _load_yml(self) -> None:
        """Load the YAML config file and store the raw data."""
        with open(self.base_config_path, "r") as f:
            self._data = yaml.safe_load(f) or {}

class Config(BaseConfig):
    """ TODO """

    def __init__(self):
        super().__init__()
        self._pipeline: PipelineConfig | None = None

    @property
    def pipeline(self) -> PipelineConfig:
        if self._pipeline is None:
            self._pipeline = PipelineConfig(self.repository, self.environment)
        return self._pipeline

    def set_repo(self, repo: Path | str) -> None:
        """ Set the repository path in the base configuration. """
        repo_path = Path(repo)
        if not repo_path.exists():
            logger.error(f"Repository path does not exist: {repo_path}")
            raise FileNotFoundError(f"Repository path does not exist: {repo_path}")
        self.repository = repo_path
        self._pipeline = None  # Reset pipeline to force re-initialization with new repo
        logger.info(f"Repository path set to: {self.repository}")

    def set_environment(self, environment: str) -> None:
        """ Set the active environment in the base configuration. """
        if environment not in ENV:
            logger.error(f"Invalid environment '{environment}'. Must be one of: {ENV}")
            raise ValueError(f"Invalid environment '{environment}'. Must be one of: {ENV}")
        self.environment = environment
        self._pipeline = None  # Reset pipeline to force re-initialization with new environment
        logger.info(f"Environment set to: {self.environment}")

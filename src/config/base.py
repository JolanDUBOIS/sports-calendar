from __future__ import annotations
from pathlib import Path
from dataclasses import dataclass

import yaml

from . import logger, CONFIG_DIR_PATH
from .constants import ENV, DEFAULT_ENV


@dataclass
class BaseConfigSpec:
    repository: Path

    def __post_init__(self):
        self.repository = Path(self.repository)
        self.validate()

    def validate(self) -> None:
        """ Validate the base configuration. """
        if not isinstance(self.repository, Path):
            logger.error("The 'repository' field must be of type 'Repository'.")
            raise ValueError("The 'repository' field must be of type 'Repository'.")

    @classmethod
    def from_yaml(cls, path: Path) -> BaseConfigSpec:
        """ Load a BaseConfig from a YAML file. """
        with open(path, 'r') as f:
            data = yaml.safe_load(f)
        return cls(**data)

class BaseConfig:
    """ TODO """

    def __init__(self):
        base_config_path = CONFIG_DIR_PATH / "base.yml"
        if not base_config_path.exists():
            logger.error(f"Base config file does not exist: {base_config_path}")
            raise FileNotFoundError(f"Base config file does not exist: {base_config_path}")
        self.base_config = BaseConfigSpec.from_yaml(base_config_path)

        self._repository = self.base_config.repository
        self._environment = DEFAULT_ENV

    @property
    def environment(self) -> str:
        return self._environment

    @property
    def repository(self) -> Path:
        return self._repository

    def set_environment(self, environment: str) -> None:
        """ Set the environment in the runtime configuration. """
        if environment not in ENV:
            logger.error(f"Invalid environment '{environment}'. Must be one of: {ENV}")
            raise ValueError(f"Invalid environment '{environment}'. Must be one of: {ENV}")
        self._environment = environment
        logger.info(f"Environment set to: {self._environment}")

    def set_repo(self, repo: Path | str) -> None:
        """ Override the repository in the configuration. """
        self._repository = Path(repo)
        logger.info(f"Repository set to: {self._repository}")
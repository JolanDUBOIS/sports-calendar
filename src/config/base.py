from . import logger, CONFIG_DIR_PATH
from .loader import load_yml
from .constants import ENV

from src.specs import (
    InfrastructureConfig,
    RuntimeConfig,
    Repository
)


class BaseConfig:
    """ TODO """

    def __init__(self):
        self.infra_config_path = CONFIG_DIR_PATH / "infrastructure.yml"
        self.runtime_config_path = CONFIG_DIR_PATH / "runtime.yml"
        self.load()

    @property
    def environment(self) -> str:
        return self.runtime.environment

    @property
    def repository(self) -> Repository:
        """ Return the active repository from the infrastructure configuration. """
        repo_name = self.runtime.repository
        for repo in self.infrastructure.repositories:
            if repo.name == repo_name:
                return repo
        logger.error(f"Active repository '{repo_name}' not found in infrastructure config.")
        raise ValueError(f"Active repository '{repo_name}' not found in infrastructure config.")

    def load(self) -> None:
        """ Load the base configuration. """
        if not self.infra_config_path.exists():
            logger.error(f"Infrastructure config file does not exist: {self.infra_config_path}")
            raise FileNotFoundError(f"Infrastructure config file does not exist: {self.infra_config_path}")

        if not self.runtime_config_path.exists():
            logger.error(f"Runtime config file does not exist: {self.runtime_config_path}")
            raise FileNotFoundError(f"Runtime config file does not exist: {self.runtime_config_path}")

        self.infrastructure = InfrastructureConfig.from_yaml(self.infra_config_path)
        self.runtime = RuntimeConfig.from_yaml(self.runtime_config_path)

    def set_repo(self, repo_name: str) -> None:
        """ Set the active repository in the runtime configuration. """
        if not any(repo.name == repo_name for repo in self.infrastructure.repositories):
            logger.error(f"Repository '{repo_name}' not found in infrastructure config.")
            raise ValueError(f"Repository '{repo_name}' not found in infrastructure config.")
        self.runtime.repository = repo_name

    def set_environment(self, environment: str) -> None:
        """ Set the environment in the runtime configuration. """
        if environment not in ENV:
            logger.error(f"Invalid environment '{environment}'. Must be one of: {ENV}")
            raise ValueError(f"Invalid environment '{environment}'. Must be one of: {ENV}")
        self.runtime.environment = environment

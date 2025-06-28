from .app_config import AppConfig
from .base_config import BaseConfig, Repository
from .credentials import Credentials
from .pipeline_config import PipelineConfig
from .secrets import Secrets


class Config:
    """ Container for all configuration components. """

    def __init__(self, repo_key: str | None = None):
        self.app = AppConfig()
        self.base = BaseConfig()
        if repo_key is not None:
            self.base.set_active_repo(repo_key)
        self.credentials = Credentials()
        self.pipeline = PipelineConfig()
        self.secrets = Secrets()

    @property
    def active_repo(self) -> Repository:
        """ Return the active repository from the base configuration. """
        return self.base.active_repo

from pathlib import Path

from .base import BaseConfig
from .pipeline import PipelineConfig


class Config:
    """ TODO """

    def __init__(self):
        self.base = BaseConfig()
        self._pipeline: PipelineConfig | None = None

    @property
    def repository(self) -> Path:
        return self.base.repository

    @property
    def environment(self) -> str:
        return self.base.environment

    @property
    def pipeline(self) -> PipelineConfig:
        if self._pipeline is None:
            self._pipeline = PipelineConfig(self.base.repository, self.base.environment)
        return self._pipeline

    def set_repo(self, repo: Path) -> None:
        """ Set the repository path in the base configuration. """
        self.base.set_repo(repo)
        self._pipeline = None  # Reset pipeline to force re-initialization with new repo

    def set_environment(self, environment: str) -> None:
        """ Set the active environment in the base configuration. """
        self.base.set_environment(environment)
        self._pipeline = None  # Reset pipeline to force re-initialization with new environment

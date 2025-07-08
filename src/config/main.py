from __future__ import annotations
from typing import TYPE_CHECKING

from . import logger
from .base import BaseConfig, Repository
from .credentials import Credentials
from .pipeline import PipelineConfig
from .secrets import Secrets

if TYPE_CHECKING:
    from src.specs import WorkflowSpec, SchemaSpec


class Config:
    """ TODO """

    def __init__(self):
        self.base = BaseConfig()
        self.credentials = Credentials()
        self.pipeline = PipelineConfig()
        self.secrets = Secrets()

    @property
    def repository(self) -> Repository:
        return self.base.repository

    @property
    def environment(self) -> str:
        return self.base.environment

    def set_repo(self, repo_name: str) -> None:
        """ Set the active repository in the base configuration. """
        self.base.set_repo(repo_name)
        logger.info(f"Repository set to: {repo_name}")
        self.pipeline.resolve_paths(self.base.repository.path)

    def set_environment(self, environment: str) -> None:
        """ Set the active environment in the base configuration. """
        self.base.set_environment(environment)
        logger.info(f"Environment set to: {environment}")

    def get_workflow(self) -> WorkflowSpec:
        """ Get the workflow configuration for the active environment. """
        return self.pipeline.get_workflow(self.base.environment)

    def get_schema(self) -> SchemaSpec:
        """ Get the schema configuration for the active environment. """
        return self.pipeline.get_schema(self.base.environment)

config = Config()
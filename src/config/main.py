from __future__ import annotations
from pathlib import Path
from typing import TYPE_CHECKING

from . import logger
from .base import BaseConfig
from .pipeline import PipelineConfig

if TYPE_CHECKING:
    from src.specs import WorkflowSpec, SchemaSpec


class Config:
    """ TODO """

    def __init__(self):
        self.base = BaseConfig()
        self.pipeline = PipelineConfig(self.base.environment)

        self._repo_set = False

    @property
    def repository(self) -> Path:
        return self.base.repository

    @property
    def environment(self) -> str:
        return self.base.environment

    def set_repo(self, repo: Path) -> None:
        """ Set the active repository in the base configuration. Must be called once before using the pipeline. Cannot be called multiple times. """
        # TODO - This behavior needs to be rethought, setting the repo shouldn't be compulsory and should work several times
        self.base.set_repo(repo)
        self.pipeline.resolve_paths(repo)

    def set_environment(self, environment: str) -> None:
        """ Set the active environment in the base configuration. """
        self.base.set_environment(environment)

    def get_workflow(self) -> WorkflowSpec:
        """ Get the workflow configuration for the active environment. """
        return self.pipeline.get_workflow()

    def get_schema(self) -> SchemaSpec:
        """ Get the schema configuration for the active environment. """
        return self.pipeline.get_schema()

config = Config()
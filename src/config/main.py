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
        """ Set the active repository in the base configuration. Must be called once before using the pipeline. Cannot be called multiple times. """
        self.base.set_repo(repo)
        self._pipeline = None  # Reset pipeline to force re-initialization with new repo

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
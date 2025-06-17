from pathlib import Path

from . import logger
from .. import ROOT_PATH
from .pipeline_stages import DataStage
from .data_processing import LayerSpec
from .data_validation import SchemaSpec


class PipelinePaths:
    """ Manages file system paths and YAML config access for the pipeline. """

    repository_mapping = {
        "test": ROOT_PATH / "data" / "repository_test",
        "prod": ROOT_PATH / "data" / "repository"
    }

    def __init__(self, repo: str = "test", config_root: Path | None = None):
        self.config = config_root or (ROOT_PATH / "config" / "pipeline_config")
        self.workflows = self.config / "workflows"
        self.schemas = self.config / "schemas"

        self.repo_path = self.repository_mapping.get(repo)
        if self.repo_path is None:
            logger.error(f"Invalid repository name: {repo}. Valid options are: {list(self.repository_mapping.keys())}")
            raise ValueError(f"Invalid repository name: {repo}. Valid options are: {list(self.repository_mapping.keys())}")

    def get_workflow_config_path(self, stage: DataStage) -> Path:
        return self.workflows / f"build_{stage}.yml"

    def get_schema_config_path(self, stage: DataStage) -> Path:
        return self.schemas / f"{stage}.yml"

class PipelineConfig:
    """ Loads and provides access to the pipeline configuration. """

    def __init__(self, repo: str = "test", config_root: Path | None = None):
        self.pipeline_paths = PipelinePaths(repo, config_root)
        self.repo_path = self.pipeline_paths.repo_path

    def get_workflow_config(self, stage: DataStage) -> LayerSpec:
        """ TODO """
        workflow_path = self.pipeline_paths.get_workflow_config_path(stage)
        layer_spec = LayerSpec.from_yaml(workflow_path)
        layer_spec.resolve_paths(self.pipeline_paths.repo_path)
        return layer_spec

    def get_schema_config(self, stage: DataStage) -> SchemaSpec:
        """ TODO """
        schema_path = self.pipeline_paths.get_schema_config_path(stage)
        schema_spec = SchemaSpec.from_yaml(schema_path)
        schema_spec.resolve_paths(self.pipeline_paths.repo_path)
        return schema_spec

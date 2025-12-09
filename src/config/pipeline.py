import traceback
from pathlib import Path
from typing import Any

from . import logger, CONFIG_DIR_PATH
from .loader import load_yml
from .constants import ENV
from src.specs import (
    WorkflowSpec,
    LayerSpec,
    SchemaSpec,
    LayerSchemaSpec
)


class BaseConfigLoader:
    """ TODO """

    def __init__(self, path: Path):
        self.path = path
        self._check_path()

    def _check_path(self):
        """ Check if the config directory exists and contains required subdirs. """
        if not self.path.exists():
            logger.error(f"Config directory does not exist: {self.path}")
            raise FileNotFoundError(f"Config directory does not exist: {self.path}")

    def _load_valid(self, path: Path) -> Any:
        """ Load and validate the configuration from the specified path. """
        configs = []
        for file in path.glob("*.yml"):
            if not file.is_file():
                logger.warning(f"Skipping non-file entry in config directory: {file}")
                continue
            configs.append(load_yml(file))
        return configs


class WorkflowsConfig(BaseConfigLoader):
    """ Loads and provides access to the workflows configuration. """

    def __init__(self, path: Path):
        super().__init__(path)
        self.workflow: WorkflowSpec = self.load(path)

    def load(self, path: Path) -> WorkflowSpec:
        """ TODO """
        configs = self._load_valid(path)
        if not configs:
            logger.debug(f"No valid workflow configurations found in {path}. Returning empty WorkflowSpec.")
            return WorkflowSpec(layers=[])
        try:
            return WorkflowSpec(layers=[LayerSpec.from_dict(config) for config in configs])
        except Exception as e:
            logger.debug("Traceback:\n%s", traceback.format_exc())
            logger.error(f"Failed to load workflows from {path}: {e}")
            raise ValueError(f"Failed to load workflows from {path}: {e}")

    def resolve_paths(self, repo_path: Path | str):
        """ Resolve paths for all workflows in the configuration. """
        for layer in self.workflow.layers:
            layer.resolve_paths(repo_path)

    def get_workflow(self) -> WorkflowSpec: # TODO - to remove
        """ TODO """
        return self.workflow


class SchemasConfig(BaseConfigLoader):
    """ Loads and provides access to the schemas configuration. """

    def __init__(self, path: Path):
        super().__init__(path)
        self.schema: SchemaSpec = self.load(path)

    def load(self, path: Path) -> SchemaSpec:
        """ Load schemas from the specified path. """
        configs = self._load_valid(path)
        if not configs:
            logger.debug(f"No valid schema configurations found in {path}. Returning empty SchemaSpec.")
            return SchemaSpec(layers=[])
        try:
            return SchemaSpec(layers=[LayerSchemaSpec.from_dict(config) for config in configs])
        except Exception as e:
            logger.debug("Traceback:\n%s", traceback.format_exc())
            logger.error(f"Failed to load schemas from {path}: {e}")
            raise ValueError(f"Failed to load schemas from {path}: {e}")

    def resolve_paths(self, repo_path: Path | str):
        """ Resolve paths for all schemas in the configuration. """
        self.schema.resolve_paths(repo_path)

    def get_schema(self) -> SchemaSpec: # TODO - to remove
        """ TODO """
        return self.schema


class ProcessorsConfig:
    """ Loads and provides access to the processors configuration. """

    def __init__(self, path: Path):
        self.path = path
        if not self.path.exists():
            logger.error(f"Processors config directory does not exist: {self.path}")
            raise FileNotFoundError(f"Processors config directory does not exist: {self.path}")

        self.processors_config = self.load()

    def load(self) -> dict[str, Any]:
        """ TODO """
        processors_config = {}
        for file in self.path.glob("*.yml"):
            if not file.is_file():
                logger.warning(f"Skipping non-file entry in processors directory: {file}")
                continue
            config = load_yml(file)
            processors_config[file.stem] = config
        return processors_config

    def get_processor(self, name: str) -> Any:
        """ Get a specific processor configuration by name. """
        if name not in self.processors_config:
            logger.error(f"Processor '{name}' not found in processors configuration.")
            raise KeyError(f"Processor '{name}' not found in processors configuration.")
        return self.processors_config[name]


class PipelineConfig:
    """ Loads and provides access to the pipeline configuration. """

    def __init__(self, repository: Path, environment: str):
        self.config_path = CONFIG_DIR_PATH / "pipeline"
        self.repository = repository
        self.environment = environment # TODO - Check valid environment (for later, when environment is removed from base maybe?? Not sure...)
        if not self.config_path.exists():
            logger.error(f"Pipeline config directory does not exist: {self.config_path}")
            raise FileNotFoundError(f"Pipeline config directory does not exist: {self.config_path}")
        self.load_sub_configs()

    def load_sub_configs(self):
        """ Load all sub-configurations for workflows, schemas, and processors. """
        self.workflows = WorkflowsConfig(self.config_path / self.environment / "workflows")
        self.workflows.resolve_paths(self.repository)  
        self.schemas = SchemasConfig(self.config_path / self.environment / "schemas")
        self.schemas.resolve_paths(self.repository)
        self.processors = ProcessorsConfig(self.config_path / "shared" / "processors")

    def get_workflow(self) -> WorkflowSpec:
        """ TODO """
        return self.workflows.get_workflow()

    def get_schema(self) -> SchemaSpec:
        """ TODO """
        return self.schemas.get_schema()

    def get_processor(self, name: str) -> dict[str, Any]:
        """ Get a specific processor configuration by name. """
        return self.processors.get_processor(name)

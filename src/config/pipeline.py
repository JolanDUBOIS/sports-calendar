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


class PipelineConfig:
    """ Loads and provides access to the pipeline configuration. """

    def __init__(self, repository: Path, environment: str):
        self.repository = repository
        self.environment = environment
        if self.environment not in ENV:
            logger.error(f"Invalid environment '{self.environment}'. Must be one of: {', '.join(ENV)}")
            raise ValueError(f"Invalid environment '{self.environment}'. Must be one of: {', '.join(ENV)}")

        self.config_path = CONFIG_DIR_PATH / "pipeline"
        if not self.config_path.exists():
            logger.error(f"Pipeline config directory does not exist: {self.config_path}")
            raise FileNotFoundError(f"Pipeline config directory does not exist: {self.config_path}")

        self.workflow = self.load_workflow(self.config_path / self.environment / "workflows")
        self.schema = self.load_schema(self.config_path / self.environment / "schemas")
        self.processors = self.load_processors(self.config_path / "shared" / "processors")

        self.workflow.resolve_paths(self.repository)
        self.schema.resolve_paths(self.repository)

    def load_workflow(self, path: Path) -> WorkflowSpec:
        """ Load workflows from the specified path. """
        configs = self._load_yaml_configs(path)
        if not configs:
            logger.debug(f"No valid workflow configurations found in {path}. Returning empty WorkflowSpec.")
            return WorkflowSpec(layers=[])
        try:
            return WorkflowSpec(layers=[LayerSpec.from_dict(config) for config in configs])
        except Exception as e:
            logger.debug("Traceback:\n%s", traceback.format_exc())
            logger.error(f"Failed to load workflows from {path}: {e}")
            raise ValueError(f"Failed to load workflows from {path}: {e}")

    def load_schema(self, path: Path) -> SchemaSpec:
        """ Load schemas from the specified path. """
        configs = self._load_yaml_configs(path)
        if not configs:
            logger.debug(f"No valid schema configurations found in {path}. Returning empty SchemaSpec.")
            return SchemaSpec(layers=[])
        try:
            return SchemaSpec(layers=[LayerSchemaSpec.from_dict(config) for config in configs])
        except Exception as e:
            logger.debug("Traceback:\n%s", traceback.format_exc())
            logger.error(f"Failed to load schemas from {path}: {e}")
            raise ValueError(f"Failed to load schemas from {path}: {e}")

    @staticmethod
    def load_processors(path: Path) -> dict[str, Any]:
        """ Load processors from the specified path. """
        processors_config = {}
        for file in path.glob("*.yml"):
            if not file.is_file():
                logger.warning(f"Skipping non-file entry in processors directory: {file}")
                continue
            config = load_yml(file)
            processors_config[file.stem] = config
        return processors_config

    @staticmethod
    def _load_yaml_configs(path: Path) -> list[dict[str, Any]]:
        """ Load and validate the configuration from the specified path. """
        configs = []
        for file in path.glob("*.yml"):
            if not file.is_file():
                logger.warning(f"Skipping non-file entry in config directory: {file}")
                continue
            configs.append(load_yml(file))
        return configs

    def get_processor(self, name: str) -> dict[str, Any]:
        """ Get a specific processor configuration by name. """
        return self.processors.get(name)

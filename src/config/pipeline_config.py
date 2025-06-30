from pathlib import Path

from . import logger, CONFIG_DIR_PATH
from src.data_pipeline import DataStage


class PipelineConfig:
    """ Loads and provides access to the pipeline configuration. """

    def __init__(self):
        self.config_path = CONFIG_DIR_PATH / "pipeline"
        self._check_config_path()

        self.workflows_path = self.config_path / "workflows"
        self.schemas_path = self.config_path / "schemas"
        self.admin_path = self.config_path / "admin"

    def _check_config_path(self):
        """ Check if the pipeline config directory and required subdirectories exist. """
        if not self.config_path.exists():
            logger.error(f"Pipeline config directory does not exist: {self.config_path}")
            raise FileNotFoundError(f"Pipeline config directory does not exist: {self.config_path}")

        required_subdirs = ["schemas", "workflows"]
        for subdir in required_subdirs:
            if not (self.config_path / subdir).exists():
                logger.error(f"Required subdirectory {subdir} does not exist in pipeline config path.")
                raise FileNotFoundError(f"Required subdirectory {subdir} does not exist in pipeline config path.")

    def get_workflow_config_path(self, stage: DataStage | str) -> Path:
        """ Get the path to the workflow configuration file for a given stage. """
        stage = str(stage)
        path = self.workflows_path / f"build_{stage}.yml"
        if not path.exists():
            logger.error(f"Workflow config file does not exist: {path}")
            raise FileNotFoundError(f"Workflow config file does not exist: {path}")
        return path

    def get_schema_config_path(self, stage: DataStage | str) -> Path:
        """ Get the path to the schema configuration file for a given stage. """
        stage = str(stage)
        path = self.schemas_path / f"{stage}.yml"
        if not path.exists():
            logger.error(f"Schema config file does not exist: {path}")
            raise FileNotFoundError(f"Schema config file does not exist: {path}")
        return path

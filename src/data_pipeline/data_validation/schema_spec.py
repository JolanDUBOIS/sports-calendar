from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path

import yaml

from . import logger
from src.datastage import DataStage
from src.config.main import config


@dataclass
class ColumnSpec:
    """ TODO """
    name: str
    type: str | None = None
    unique: bool = False
    nullable: bool = True

    def __post_init__(self):
        """ Validate the column specification. """
        if not self.name:
            logger.error("Column name cannot be empty.")
            raise ValueError("Column name cannot be empty.")
        if self.type and not isinstance(self.type, str):
            logger.error("Column type must be a string or None.")
            raise TypeError("Column type must be a string or None.")
        if not isinstance(self.unique, bool):
            logger.error("Column unique must be a boolean.")
            raise TypeError("Column unique must be a boolean.")
        if not isinstance(self.nullable, bool):
            logger.error("Column nullable must be a boolean.")
            raise TypeError("Column nullable must be a boolean.")

    @classmethod
    def from_dict(cls, d: dict) -> ColumnSpec:
        """ Create a ColumnSpec from a dictionary. """
        return cls(
            name=d["name"],
            type=d.get("type"),
            unique=d.get("unique", False),
            nullable=d.get("nullable", True)
        )

@dataclass
class ModelSchemaSpec:
    """ TODO """
    name: str
    path: Path
    columns: list[ColumnSpec]

    def __post_init__(self):
        """ Validate the model specification. """
        if not self.name:
            logger.error("Model name cannot be empty.")
            raise ValueError("Model name cannot be empty.")
        if not self.path or not isinstance(self.path, Path):
            logger.error("Model path must be a valid Path object.")
            raise ValueError("Model path must be a valid Path object.")
        if not isinstance(self.columns, list):
            logger.error("Model columns must be a list.")
            raise TypeError("Model columns must be a list.")
        if not all(isinstance(col, ColumnSpec) for col in self.columns):
            logger.error("All elements in model columns must be ColumnSpec instances.")
            raise TypeError("All elements in model columns must be ColumnSpec instances.")

    @classmethod
    def from_dict(cls, d: dict) -> ModelSchemaSpec:
        """ Create a ModelSchemaSpec from a dictionary. """
        return cls(
            name=d["name"],
            path=Path(d["path"]),
            columns=[ColumnSpec.from_dict(col) for col in d.get("columns", [])]
        )

@dataclass
class SchemaSpec:
    """ TODO """
    name: str
    stage: str
    models: list[ModelSchemaSpec]
    description: str | None = None

    def __post_init__(self):
        """ Validate the schema specification. """
        if not self.name:
            logger.error("Schema name cannot be empty.")
            raise ValueError("Schema name cannot be empty.")
        if not self._is_valid_stage(self.stage):
            logger.error(f"Invalid stage name: {self.stage}")
            raise ValueError(f"Invalid stage name: {self.stage}")
        if not isinstance(self.models, list):
            logger.error("Models must be a list.")
            raise TypeError("Models must be a list.")
        if not all(isinstance(model, ModelSchemaSpec) for model in self.models):
            logger.error("All elements in models must be ModelSchemaSpec instances.")
            raise TypeError("All elements in models must be ModelSchemaSpec instances.")
    
        self._resolve_paths(config.repository.path)

    def _resolve_paths(self, base_path: Path) -> None:
        """ Resolve model paths relative to the base path. """
        for model in self.models:
            model.path = base_path / self.stage / model.path

    def get(self, model: str) -> ModelSchemaSpec | None:
        """ Get a model schema by name. """
        for model_spec in self.models:
            if model_spec.name == model:
                return model_spec
        logger.warning(f"Model '{model}' not found in schema '{self.name}'.")
        return None

    @staticmethod
    def _is_valid_stage(stage_name: str) -> bool:
        try:
            DataStage(stage_name)
            return True
        except ValueError:
            return False

    @classmethod
    def from_dict(cls, d: dict) -> SchemaSpec:
        """ Create a SchemaSpec from a dictionary. """
        return cls(
            name=d["name"],
            stage=d["stage"],
            models=[ModelSchemaSpec.from_dict(model) for model in d.get("models", [])],
            description=d.get("description")
        )

    @classmethod
    def from_yaml(cls, yaml_path: str | Path) -> SchemaSpec:
        """ Create a SchemaSpec from a YAML file. """
        yaml_path = Path(yaml_path)
        with yaml_path.open(mode='r') as file:
            data = yaml.safe_load(file)
        return cls.from_dict(data)

from pathlib import Path
from typing import Iterable
from dataclasses import dataclass

from . import logger
from .model import ModelSchemaSpec
from src.specs import BaseModel
from src.utils import DataStage


@dataclass
class LayerSchemaSpec(BaseModel):
    name: str
    stage: DataStage
    models: list[ModelSchemaSpec]
    description: str | None = None

    def __iter__(self) -> Iterable[ModelSchemaSpec]:
        """ Iterate over the models in the schema. """
        return iter(self.models)

    def __contains__(self, name: str) -> bool:
        """ Check if a model with the given name exists in the schema. """
        return any(model.name == name for model in self.models)

    def __getitem__(self, name: str) -> ModelSchemaSpec:
        """ Get a model by its name. """
        return self.get(name)

    def get(self, name: str) -> ModelSchemaSpec:
        """ Get a model by its name. """
        for model in self.models:
            if model.name == name:
                return model
        logger.error(f"Model with name '{name}' not found in schema '{self.name}'.")
        raise KeyError(f"Model with name '{name}' not found in schema '{self.name}'.")

    def resolve_paths(self, base_path: Path | str) -> None:
        """ Resolve the paths of all models in the schema relative to a base path. """
        for model in self.models:
            model.path = model.resolve_path(base_path)

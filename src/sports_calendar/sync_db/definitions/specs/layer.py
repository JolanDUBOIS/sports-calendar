from __future__ import annotations
from pathlib import Path
from typing import Iterable
from dataclasses import dataclass

from . import logger
from .model import ModelSpec
from sports_calendar.sc_core import SpecModel, DataStage


@dataclass
class LayerSpec(SpecModel):
    name: str
    stage: DataStage
    models: list[ModelSpec]
    description: str | None = None

    def __iter__(self) -> Iterable[ModelSpec]:
        """ Iterate over the models in the layer. """
        return iter(self.models)

    def __contains__(self, name: str) -> bool:
        """ Check if a model with the given name exists in the layer. """
        return any(model.name == name for model in self.models)

    def __getitem__(self, name: str) -> ModelSpec:
        """ Get a model by its name. """
        return self.get(name)

    def validate(self) -> None:
        """ Validate the layer. """
        if not isinstance(self.stage, DataStage):
            logger.error(f"Invalid stage '{self.stage}' in layer '{self.name}'. Expected a DataStage instance.")
            raise ValueError(f"Invalid stage '{self.stage}' in layer '{self.name}'. Expected a DataStage instance.")
        if not all(isinstance(model, ModelSpec) for model in self.models):
            logger.error(f"All models in layer '{self.name}' must be instances of ModelSpec.")
            raise ValueError(f"All models in layer '{self.name}' must be instances of ModelSpec.")
        self._check_unique_model_names()

    def get(self, name: str) -> ModelSpec:
        """ Get a model by its name. """
        for model in self.models:
            if model.name == name:
                return model
        logger.error(f"Model with name '{name}' not found in layer '{self.name}'.")
        raise KeyError(f"Model with name '{name}' not found in layer '{self.name}'.")

    def resolve_paths(self, base_path: Path | str) -> None:
        """ Resolve the paths of all models in the layer relative to a base path. """
        for model in self.models:
            model.resolve_paths(base_path)

    def _check_unique_model_names(self):
        seen = set()
        duplicates = set()
        for m in self.models:
            if m.name in seen:
                duplicates.add(m.name)
            else:
                seen.add(m.name)
        if duplicates:
            logger.error(f"Duplicate model names found: {', '.join(duplicates)} in layer '{self.name}'.")
            raise ValueError(f"Duplicate model names found: {', '.join(duplicates)} in layer '{self.name}'")

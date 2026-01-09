from pathlib import Path
from typing import Iterable
from dataclasses import dataclass

from . import logger
from .layer import LayerSchemaSpec
from sports_calendar.core import SpecModel, DataStage


@dataclass
class SchemaSpec(SpecModel):
    layers: list[LayerSchemaSpec]

    def __iter__(self) -> Iterable[LayerSchemaSpec]:
        """ Iterate over the layers in the schema ordered by stage. """
        return iter(self.layers)

    def __contains__(self, stage: DataStage) -> bool:
        """ Check if a layer with the given stage exists in the schema. """
        stage = DataStage(stage) if isinstance(stage, str) else stage
        return any(layer.stage == stage for layer in self.layers)

    def __getitem__(self, stage: DataStage) -> LayerSchemaSpec:
        """ Get a layer by its stage. """
        return self.get(stage)

    def get(self, stage: DataStage) -> LayerSchemaSpec:
        """ Get a layer by its stage. """
        stage = DataStage(stage) if isinstance(stage, str) else stage
        for layer in self.layers:
            if layer.stage == stage:
                return layer
        logger.error(f"Layer with stage '{stage}' not found in schema.")
        raise KeyError(f"Layer with stage '{stage}' not found in schema.")

    def resolve_paths(self, base_path: Path | str) -> None:
        """ Resolve the paths of all layers in the schema relative to a base path. """
        for layer in self.layers:
            layer.resolve_paths(base_path)

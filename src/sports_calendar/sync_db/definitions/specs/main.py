from __future__ import annotations
from pathlib import Path
from typing import Iterable
from dataclasses import dataclass

from . import logger
from .layer import LayerSpec
from sports_calendar.sc_core import SpecModel, DataStage


@dataclass
class WorkflowSpec(SpecModel):
    layers: list[LayerSpec]

    def __iter__(self) -> Iterable[LayerSpec]:
        """ Iterate over the layers in the workflow ordered by stage. """
        return iter(sorted(self.layers, key=lambda layer: layer.stage.value))

    def __contains__(self, stage: DataStage | str) -> bool:
        """ Check if a layer with the given stage exists in the workflow. """
        stage = DataStage(stage) if isinstance(stage, str) else stage
        return any(layer.stage == stage for layer in self.layers)

    def __getitem__(self, stage: DataStage | str) -> LayerSpec:
        """ Get a layer by its stage. """
        return self.get(stage)

    def validate(self) -> None:
        """ Validate the workflow. """
        if not all(isinstance(layer, LayerSpec) for layer in self.layers):
            logger.error("All layers in the workflow must be instances of LayerSpec.")
            raise ValueError("All layers in the workflow must be instances of LayerSpec.")
        self._check_unique_layer_stages()

    def get(self, stage: DataStage | str) -> LayerSpec:
        """ Get a layer by its stage. """
        stage = DataStage(stage) if isinstance(stage, str) else stage
        for layer in self.layers:
            if layer.stage == stage:
                return layer
        logger.error(f"Layer with stage '{stage}' not found in workflow.")
        raise KeyError(f"Layer with stage '{stage}' not found in workflow.")

    def resolve_paths(self, base_path: Path | str) -> None:
        """ Resolve the paths of all layers in the workflow relative to a base path. """
        logger.debug(f"Resolving workflow paths with base path: {base_path}")
        for layer in self.layers:
            layer.resolve_paths(base_path)

    def _check_unique_layer_stages(self):
        seen = set()
        duplicates = set()
        for layer in self.layers:
            if layer.stage in seen:
                duplicates.add(layer.stage)
            else:
                seen.add(layer.stage)
        if duplicates:
            logger.error(f"Duplicate layer stages found: {', '.join(map(str, duplicates))}.")
            raise ValueError(f"Duplicate layer stages found: {', '.join(map(str, duplicates))}")

from __future__ import annotations
import traceback
from pathlib import Path
from dataclasses import dataclass

from . import logger
from .order_models import ModelOrder, DataStage
from .managers import ModelManager, ModelSpec
from ..utils import read_yml_file


@dataclass
class LayerSpec:
    name: str
    stage: str
    models: list[ModelSpec]
    description: str | None = None

    def __post_init__(self):
        """ Validate the layer specification. """
        if not self.name:
            logger.error("Layer name cannot be empty.")
            raise ValueError("Layer name cannot be empty.")
        if not self._is_valid_stage(self.stage):
            logger.error(f"Invalid stage name: {self.stage}")
            raise ValueError(f"Invalid stage name: {self.stage}")

    @staticmethod
    def _is_valid_stage(stage_name: str) -> bool:
        try:
            DataStage.from_str(stage_name)
            return True
        except ValueError:
            return False

    @classmethod
    def from_dict(cls, d: dict) -> LayerSpec:
        """ Create a LayerSpec from a dictionary. """
        return cls(
            name=d["name"],
            stage=d["stage"],
            models=[ModelSpec.from_dict(model) for model in d["models"]],
            description=d.get("description")
        )

class LayerBuilder:
    """ TODO """

    def __init__(self, layer_spec: LayerSpec, repo_path: str | Path):
        """ Initialize the LayerBuilder with a layer specification. """
        self.layer_spec = layer_spec
        self.repo_path = Path(repo_path)
        self.models_order = ModelOrder(self.layer_spec.models, self.layer_spec.stage)

    def build(self, manual: bool = False) -> None:
        """ TODO """
        for model in self.models_order:
            try:
                model_manager = ModelManager(model, self.repo_path)
                model_manager.run(manual=manual)
            except Exception as e:
                logger.error(f"Error processing model {model.name}: {e}")
                logger.debug(traceback.format_exc())
                self.models_order.mark_failed(model)

    @classmethod
    def from_dict(cls, d: dict, repo_path: str | Path) -> LayerBuilder:
        """ Create a LayerBuilder from a dictionary. """
        layer_spec = LayerSpec.from_dict(d)
        return cls(layer_spec, repo_path)

    @classmethod
    def from_yaml(cls, yaml_path: str | Path, repo_path: str | Path) -> LayerBuilder:
        """ Create a LayerBuilder from a YAML file. """
        layer_data = read_yml_file(yaml_path)
        return cls.from_dict(layer_data, repo_path)

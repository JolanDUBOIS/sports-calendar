from __future__ import annotations
import traceback
from pathlib import Path
from dataclasses import dataclass

import yaml

from . import logger
from .order_models import ModelOrder
from .managers import ModelManager, ModelSpec
from ..pipeline_stages import DataStage


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

    def get(self, model: str) -> ModelSpec | None:
        """ Retrieve a model specification by its name. """
        for model_spec in self.models:
            if model_spec.name == model:
                return model_spec
        logger.warning(f"Model {model} not found in layer {self.name}.")
        return None

    def resolve_paths(self, base_path: Path) -> None:
        """ Resolve model paths relative to the base path. """
        for model in self.models:
            try:
                model.resolve_paths(base_path)
            except Exception as e:
                logger.error(f"Error resolving path for model {model.name}: {e}")
                logger.debug(traceback.format_exc())
                raise ValueError(f"Failed to resolve path for model {model.name}: {e}")

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

    @classmethod
    def from_yaml(cls, yaml_path: str | Path) -> LayerSpec:
        """ Create a LayerSpec from a YAML file. """
        yaml_path = Path(yaml_path)
        with yaml_path.open(mode='r') as file:
            data = yaml.safe_load(file)
        return cls.from_dict(data)

class LayerBuilder:
    """
    Responsible for constructing and executing a sequence of models defined in a layer specification.

    This class manages the lifecycle of models within a specific layer of the pipeline, 
    ensuring they are executed in the correct order according to the stage and model dependencies.

    Attributes:
        layer_spec (LayerSpec): Specification object detailing the models and their configuration for this layer.
        repo_path (Path): Filesystem path to the repository root where models and resources reside.
        models_order (ModelOrder): Ordered iterable of models respecting dependencies and stage order.

    Methods:
        build(manual: bool = False) -> None:
            Executes each model in the layer in the predefined order, handling errors and marking failures.
        
        from_dict(d: dict, repo_path: str | Path) -> LayerBuilder:
            Factory method to create a LayerBuilder instance from a dictionary specification.
    """

    def __init__(self, layer_spec: LayerSpec, repo_path: str | Path):
        """ Initialize the LayerBuilder with a layer specification. """
        self.layer_spec = layer_spec
        self.repo_path = Path(repo_path)
        self.models_order = ModelOrder(self.layer_spec.models, self.layer_spec.stage)

    def build(self, **kwargs) -> None:
        """
        Execute all models in this layer according to the specified order.

        This method runs each model's manager sequentially, passing the `manual` flag 
        to control manual override behaviors if applicable. It captures and logs any 
        exceptions per model to avoid stopping the entire layer execution, marking 
        failed models for later inspection or retry.

        Args:
            TODO
            (in kwargs, manual, dry_run, verbose, reset, etc.)
        """
        logger.info(f"Building layer '{self.layer_spec.stage}'.")
        for model_spec in self.models_order:
            try:
                model_manager = ModelManager(model_spec, self.repo_path)
                model_manager.run(**kwargs)
            except Exception as e:
                logger.error(f"Error processing model {model_spec.name}: {e}")
                logger.debug(traceback.format_exc())
                self.models_order.mark_failed(model_spec)
        logger.info(f"Completed building layer '{self.layer_spec.stage}'.")

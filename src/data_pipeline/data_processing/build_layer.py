from __future__ import annotations
import traceback
from dataclasses import dataclass

from . import logger
from .order_models import ModelOrder
from .managers import ModelManager
from src.specs import LayerSpec


class LayerBuilder:
    """
    Responsible for constructing and executing a sequence of models defined in a layer specification.

    This class manages the lifecycle of models within a specific layer of the pipeline, 
    ensuring they are executed in the correct order according to the stage and model dependencies.

    Attributes:
        layer_spec (LayerSpec): Specification object detailing the models and their configuration for this layer.
        models_order (ModelOrder): Ordered iterable of models respecting dependencies and stage order.

    Methods:
        build(manual: bool = False) -> None:
            Executes each model in the layer in the predefined order, handling errors and marking failures.
        
        from_dict(d: dict) -> LayerBuilder:
            Factory method to create a LayerBuilder instance from a dictionary specification.
    """

    def __init__(self, layer_spec: LayerSpec):
        """ Initialize the LayerBuilder with a layer specification. """
        self.layer_spec = layer_spec
        self.models_order = ModelOrder(self.layer_spec.models, self.layer_spec.stage)
        logger.debug(f"Initialized LayerBuilder with layer spec ({self.layer_spec}) and models order ({self.models_order}).")

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
                logger.debug(f"Building model: {model_spec.name} in layer '{self.layer_spec.stage}'")
                model_manager = ModelManager(model_spec)
                model_manager.run(**kwargs)
            except Exception as e:
                logger.error(f"Error processing model {model_spec.name}: {e}")
                logger.debug(traceback.format_exc())
                self.models_order.mark_failed(model_spec)
        logger.info(f"Completed building layer '{self.layer_spec.stage}'.")

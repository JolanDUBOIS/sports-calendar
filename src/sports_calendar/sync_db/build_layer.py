from __future__ import annotations

from . import logger
from .order_models import ModelOrder
from .managers import ModelManager
from .definitions.specs import LayerSpec


class LayerBuilder:
    """ TODO """

    def __init__(self, layer_spec: LayerSpec):
        """ Initialize the LayerBuilder with a layer specification. """
        self.layer_spec = layer_spec
        self.models_order = ModelOrder(self.layer_spec.models, self.layer_spec.stage)
        logger.debug(f"Initialized LayerBuilder with layer spec ({self.layer_spec}) and models order ({self.models_order}).")

    def build(self, **kwargs) -> None:
        """ TODO """
        logger.info(f"Building layer '{self.layer_spec.stage}'.")
        for model_spec in self.models_order:
            try:
                logger.debug(f"Building model: {model_spec.name} in layer '{self.layer_spec.stage}'")
                model_manager = ModelManager(model_spec)
                model_manager.run(**kwargs)
            except Exception:
                logger.exception(f"Error processing model {model_spec.name} in layer '{self.layer_spec.stage}'. Marking as failed.")
                self.models_order.mark_failed(model_spec)
        logger.info(f"Completed building layer '{self.layer_spec.stage}'.")

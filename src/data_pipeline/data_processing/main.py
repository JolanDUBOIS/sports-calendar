import traceback

from . import logger
from .build_layer import LayerBuilder, ModelManager, LayerSpec
from .. import DataStage
from src.config.manager import pipeline_config


def run_pipeline(
    stage: DataStage | None = None,
    model: str | None = None,
    **kwargs
) -> None:
    # Single model
    if model:
        layer_spec = LayerSpec.from_yaml(pipeline_config.get_workflow_config_path(stage))
        model_spec = layer_spec.get(model)        
        if not model_spec:
            logger.error(f"Model '{model}' not found in layer '{layer_spec.name}'.")
            raise ValueError(f"Model '{model}' not found in layer '{layer_spec.name}'.")
        try:
            model_manager = ModelManager(model_spec)
            model_manager.run(**kwargs)
        except Exception as e:
            logger.error(f"Error processing model {model_spec.name}: {e}")
            logger.debug("Traceback:\n%s", traceback.format_exc())

    # Multiple models
    else:
        stages = [stage] if stage is not None else DataStage.instances()
        for _stage in stages:
            layer_spec = LayerSpec.from_yaml(pipeline_config.get_workflow_config_path(_stage))
            builder = LayerBuilder(layer_spec)
            builder.build(**kwargs)

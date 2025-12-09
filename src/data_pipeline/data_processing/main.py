import traceback

from . import logger
from .build_layer import LayerBuilder, ModelManager
from src.utils import DataStage
from src.config.main import config


def run_pipeline(
    stage: DataStage | None = None,
    model: str | None = None,
    **kwargs
) -> None:
    logger.debug(f"Running pipeline with stage: {stage}, model: {model}, kwargs: {kwargs}")
    # Single model
    if model:
        logger.debug(f"Processing single model: {model} in stage: {stage}")
        layer_spec = config.pipeline.workflow.get(stage)
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
        logger.debug(f"Processing all models (stage: {stage}).")
        stages = [stage] if stage is not None else DataStage.instances()
        for _stage in stages:
            logger.debug(f"Processing stage: {_stage}")
            layer_spec = config.pipeline.workflow.get(_stage)
            builder = LayerBuilder(layer_spec)
            builder.build(**kwargs)

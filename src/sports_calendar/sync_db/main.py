from . import logger
from .definitions import load_workflow
from .build_layer import LayerBuilder, ModelManager
from sports_calendar.core import DataStage, Paths


def run_pipeline(
    stage: DataStage | None = None,
    model: str | None = None,
    **kwargs
) -> None:
    logger.debug(f"Running pipeline with stage: {stage}, model: {model}, kwargs: {kwargs}")
    workflow = load_workflow(strict=True)

    workflow.resolve_paths(base_path=Paths.DB_DIR)

    # Single model
    if model:
        logger.debug(f"Processing single model: {model} in stage: {stage}")
        layer_spec = workflow.get(stage)
        model_spec = layer_spec.get(model)        
        if not model_spec:
            logger.error(f"Model '{model}' not found in layer '{layer_spec.name}'.")
            raise ValueError(f"Model '{model}' not found in layer '{layer_spec.name}'.")
        try:
            model_manager = ModelManager(model_spec)
            model_manager.run(**kwargs)
        except Exception:
            logger.exception(f"Error processing model {model_spec.name} in layer '{layer_spec.name}'.")

    # Multiple models
    else:
        logger.debug(f"Processing all models (stage: {stage}).")
        stages = [stage] if stage is not None else DataStage.instances()
        for _stage in stages:
            logger.debug(f"Processing stage: {_stage}")
            layer_spec = workflow.get(_stage)
            builder = LayerBuilder(layer_spec)
            builder.build(**kwargs)

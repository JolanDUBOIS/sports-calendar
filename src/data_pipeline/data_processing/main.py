import traceback

from . import logger
from .build_layer import LayerBuilder, ModelManager, LayerSpec
from src.config import StageManager
from src.config.manager import pipeline_config, base_config


def run_pipeline(
    stage_manager: StageManager | None = None,
    model: str | None = None,
    **kwargs
) -> None:
    active_repo = base_config.active_repo
    # Single model
    if model:
        layer_spec = LayerSpec.from_yaml(pipeline_config.get_workflow_config_path(stage_manager))
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
        stage_managers = active_repo.get_stages() if stage_manager is None else [stage_manager]
        for _stage_manager in stage_managers:
            layer_spec = LayerSpec.from_yaml(pipeline_config.get_workflow_config_path(_stage_manager))
            builder = LayerBuilder(layer_spec)
            builder.build(**kwargs)

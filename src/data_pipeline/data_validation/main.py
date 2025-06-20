import traceback

from . import logger
from .schema_manager import LayerSchemaManager, ModelSchemaManager
from .validation_result import SchemaValidationResult
from .schema_spec import SchemaSpec
from src.config import StageManager
from src.config.manager import pipeline_config, base_config


def run_validation(
    stage_manager: StageManager | None = None,
    model: str | None = None,
    **kwargs
) -> list[SchemaValidationResult]:
    active_repo = base_config.active_repo
    # Single model
    if model:
        schema_spec = SchemaSpec.from_yaml(pipeline_config.get_schema_config_path(stage_manager))
        model_spec = schema_spec.get(model)
        if not model_spec:
            logger.error(f"Model '{model}' not found in schema '{schema_spec.name}'.")
            raise ValueError(f"Model '{model}' not found in schema '{schema_spec.name}'.")
        try:
            model_manager = ModelSchemaManager(model_spec)
            results = model_manager.validate(**kwargs)
            return SchemaValidationResult(schema=schema_spec.name, results=[results])
        except Exception as e:
            logger.error(f"Validation failed for model '{model}': {e}")
            logger.debug("Traceback:\n%s", traceback.format_exc())

    # Multiple models
    else:
        stage_managers = active_repo.get_stages() if stage_manager is None else [stage_manager]
        results = []
        for _stage_manager in stage_managers:
            schema_spec = SchemaSpec.from_yaml(pipeline_config.get_schema_config_path(_stage_manager))
            layer_manager = LayerSchemaManager(schema_spec)
            results.append(layer_manager.validate(**kwargs))
        return results

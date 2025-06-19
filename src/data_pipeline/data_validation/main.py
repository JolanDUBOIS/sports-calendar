import traceback

from . import logger
from .schema_manager import LayerSchemaManager, ModelSchemaManager
from .validation_result import SchemaValidationResult
from .schema_spec import SchemaSpec
from .. import DataStage
from src.config.manager import pipeline_config


def run_validation(
    stage: DataStage | None = None,
    model: str | None = None,
    **kwargs
) -> list[SchemaValidationResult]:
    # Single model
    if model:
        schema_spec = SchemaSpec.from_yaml(pipeline_config.get_schema_config_path(stage))
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
        stages = [stage] if stage is not None else DataStage.instances()
        results = []
        for _stage in stages:
            schema_spec = SchemaSpec.from_yaml(pipeline_config.get_schema_config_path(_stage))
            layer_manager = LayerSchemaManager(schema_spec)
            results.append(layer_manager.validate(**kwargs))
        return results

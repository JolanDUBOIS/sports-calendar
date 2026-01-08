from . import logger
from .definitions import load_schema
from .schema_manager import LayerSchemaManager, ModelSchemaManager
from .validation_result import SchemaValidationResult
from sports_calendar.sc_core import DataStage, Paths


def run_validation(
    stage: DataStage | None = None,
    model: str | None = None,
    **kwargs
) -> list[SchemaValidationResult]:
    logger.debug(f"Running schema validation with stage: {stage}, model: {model}, kwargs: {kwargs}")
    schema = load_schema(strict=True)

    schema.resolve_paths(base_path=Paths.DB_DIR)

    # Single model
    if model:
        schema_spec = schema.get(stage)
        model_spec = schema_spec.get(model)
        if not model_spec:
            logger.error(f"Model '{model}' not found in schema '{schema_spec.name}'.")
            raise ValueError(f"Model '{model}' not found in schema '{schema_spec.name}'.")
        try:
            model_manager = ModelSchemaManager(model_spec)
            results = model_manager.validate(**kwargs)
            return SchemaValidationResult(schema=schema_spec.name, results=[results])
        except Exception:
            logger.exception(f"Validation failed for model '{model}'.")

    # Multiple models
    else:
        stages = [stage] if stage is not None else DataStage.instances()[1:] # Exclude LANDING
        results = []
        for _stage in stages:
            logger.debug(f"Validating stage: {_stage}")
            schema_spec = schema.get(_stage)
            layer_manager = LayerSchemaManager(schema_spec)
            results.append(layer_manager.validate(**kwargs))
        return results

from __future__ import annotations
from typing import TYPE_CHECKING
import traceback

from . import logger
from .schema_manager import LayerSchemaManager, ModelSchemaManager
from .validation_result import SchemaValidationResult
from .. import DataStage
if TYPE_CHECKING:
    from .. import PipelineConfig


def run_validation(
    pipeline_config: PipelineConfig,
    stage: DataStage | None = None,
    model: str | None = None,
    **kwargs
) -> list[SchemaValidationResult]:
    # Single model
    if model:
        schema_spec = pipeline_config.get_schema_config(stage)
        model_spec = schema_spec.get(model)
        if not model_spec:
            logger.error(f"Model '{model}' not found in schema '{schema_spec.name}'.")
            raise ValueError(f"Model '{model}' not found in schema '{schema_spec.name}'.")
        try:
            model_manager = ModelSchemaManager(model_spec, pipeline_config.repo_path)
            results = model_manager.validate(**kwargs)
            return SchemaValidationResult(schema=schema_spec.name, results=[results])
        except Exception as e:
            logger.error(f"Validation failed for model '{model}': {e}")
            logger.debug(traceback.format_exc())

    # Multiple models
    else:
        stages = [stage] if stage is None else DataStage.instances()
        results = []
        for _stage in stages:
            schema_spec = pipeline_config.get_schema_config(_stage)
            layer_manager = LayerSchemaManager(schema_spec, pipeline_config.repo_path)
            results.append(layer_manager.validate(**kwargs))
        return results

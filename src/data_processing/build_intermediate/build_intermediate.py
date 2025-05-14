from pathlib import Path

import pandas as pd

from .pipelines import PipelineBaseClass
from src.data_processing import logger
from src.data_processing.utils import (
    inject_static_fields,
    order_models,
    load_sources,
    enforce_schema,
    get_subclass
)
from src.data_processing.file_io import FileHandlerFactory


def build_intermediate(db_repo: str, instructions: dict, schemas: dict, manual: bool = False):
    """ TODO """
    logger.info("Building intermediate...")
    db_repo = Path(db_repo)

    models = instructions.get("models", [])
    models = order_models(models, "intermediate")
    for model in models:
        # Trigger
        model_trigger = model.get("trigger", "automatic")
        if not manual and model_trigger == "manual":
            continue
        if model_trigger != "automatic":
            logger.warning(f"Unsupported trigger type: {model_trigger}.")
            continue

        # Name
        model_name = model.get("name")
        logger.info(f"Building intermediate for model: {model_name}")

        # Output handler
        output = model.get("output", {})
        output_handler = FileHandlerFactory.create_file_handler(db_repo / output.get("path"))
        output_last_update = output_handler.last_update()

        # Source handlers
        sources = model.get("sources", [])
        loaded_sources = load_sources(sources, db_repo, version_threshold=output_last_update)
        logger.debug(f"Loaded sources: {loaded_sources.keys()}")

        # Processing
        processing = model.get("processing", [])
        processor_name = processing.get("processor")
        processor = get_subclass(PipelineBaseClass, processor_name)
        if not processor:
            logger.error(f"Processor {processor_name} not found.")
            raise ValueError(f"Processor {processor_name} not found.")
        data = processor.run(sources=loaded_sources, **processing.get("params", {}))
        if not isinstance(data, pd.DataFrame):
            logger.error(f"The output data should be a pandas DataFrame.")
            raise ValueError(f"The output data should be a pandas DataFrame.")
        logger.debug(f"Data columns: {data.columns}")

        # Inject static fields
        static_fields = model.get("static_fields", {})
        if static_fields:
            data = inject_static_fields(data, static_fields)

        # Enforce schema
        schema_name = output.get("schema")
        try:
            schema = next(model.get("columns") for model in schemas.get('models') if model.get("name") == schema_name)
        except StopIteration:
            logger.error(f"Schema {schema_name} not found.")
            raise ValueError(f"Schema {schema_name} not found.")
        data = enforce_schema(data, schema, strict=False)

        # Write Output
        output_handler.write(data, overwrite=output.get("overwrite", False))
        logger.info(f"Output written to: {output_handler.path}")

    logger.info("Intermediate build completed.")

from datetime import datetime
from pathlib import Path

import pandas as pd

from .extract_json import extract_from_json
from .extract_tables import extract_parallel_entities
from src.data_processing import logger
from src.data_processing.utils import order_models, inject_static_fields
from src.data_processing.file_io import FileHandlerFactory


EXTRACTION_FUNCTION_REGISTRY = {
    "extract_from_json": extract_from_json,
    "extract_parallel_entities": extract_parallel_entities
}

def save_file(file_path: Path, data: pd.DataFrame):
    """ TODO """
    if not file_path.suffix == ".csv":
        logger.error(f"File path {file_path} is not a CSV file.")
        raise ValueError(f"File path {file_path} is not a CSV file.")
    
    if not file_path.exists():
        data.to_csv(file_path, mode='w', header=True, index=False)
    else:
        data.to_csv(file_path, mode='a', header=False, index=False)
    
    logger.info(f"Data appended to {file_path}")

def extraction_workflow(db_repo: str, instructions: dict, manual: bool = False):
    """ TODO """
    # TODO - Use the depends_on parameter to order the extraction
    logger.info("Starting extraction workflow...")
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

        model_name = model.get("name")
        logger.info(f"Extraction for model: {model_name}")
        
        # Output handler
        output = model.get("output", {})
        output_handler = FileHandlerFactory.create_file_handler(db_repo / output.get("path"))
        output_handler.add_schema(output.get("columns", []))
        output_last_update = output_handler.last_update()

        # Source handlers
        sources = model.get("sources", [])
        loaded_sources = {}
        for source in sources:
            source_name = source.get("name")
            source_path = source.get("path")
            source_versioning = source.get("versioning", {})
            version_mode = source_versioning.get("mode", "all")
            version_on = source_versioning.get("on", "created_at")
            version_type = source_versioning.get("version_type", "datetime")

            file_handler = FileHandlerFactory.create_file_handler(db_repo / source_path)
            loaded_sources[source_name] = file_handler.read(
                mode=version_mode,
                on=version_on,
                version_threshold=output_last_update,
                version_type=version_type
            )
        logger.debug(f"Loaded sources: {loaded_sources.keys()}")
        logger.debug(f"Loaded sources data: {loaded_sources}")

        # Extraction
        extraction_func = EXTRACTION_FUNCTION_REGISTRY.get(model.get("function"))
        logger.debug(f"Extraction function: {extraction_func}")
        if not extraction_func:
            logger.error(f"Unsupported extraction function: {model.get('function')}.")
            raise ValueError(f"Unsupported extraction function: {model.get('function')}.")
        list_params = model.get("params", [])

        output = pd.DataFrame()
        for params in list_params:
            logger.debug(f"Extraction params: {params}")
            new_output = extraction_func(sources = loaded_sources, **params)
            logger.debug(f"New output: {new_output}")
            if not isinstance(output, pd.DataFrame):
                logger.error(f"Output from extraction function is not a DataFrame.")
                raise ValueError(f"Output from extraction function is not a DataFrame.")
            output = pd.concat([output, new_output], ignore_index=True)

        static_fields = model.get("static_fields", [])
        output = inject_static_fields(output, static_fields)        
        output_handler.write(data=output, overwrite=output.get("overwrite", False))

    logger.info("Extraction workflow completed.")

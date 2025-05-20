from pathlib import Path

import pandas as pd

from . import logger
from .processors import ProcessorFactory
from src.data_pipeline.file_io import FileHandlerFactory


def validate_trigger(trigger: str, manual: bool) -> bool:
    """ TODO """
    if trigger not in ["automatic", "manual"]:
        logger.warning(f"Unsupported trigger type: {trigger}.")
        return False
    elif trigger == "manual" and not manual:
        logger.info(f"Skipping model with manual trigger: {trigger}.")
        return False
    return True

def load_sources(sources: list[dict], db_repo: Path, version_threshold: any = None) -> dict[str, dict|pd.DataFrame]:
    """ TODO """
    loaded_sources = {}
    for source in sources:
        source_name = source.get("name")
        source_path = source.get("path")
        source_versioning = source.get("versioning", {})
        version_mode = source_versioning.get("mode", "all")
        version_on = source_versioning.get("on", "created_at")

        file_handler = FileHandlerFactory.create_file_handler(db_repo / source_path)
        loaded_sources[source_name] = file_handler.read(
            mode=version_mode,
            on=version_on,
            version_threshold=version_threshold
        )
    
    return loaded_sources

def inject_static_fields(data: list[dict]|pd.DataFrame, static_fields: list[dict]) -> list[dict]|pd.DataFrame:
    """ Inject static fields into the data. """
    if isinstance(data, list):
        return _inject_static_fields_json(data, static_fields)
    elif isinstance(data, pd.DataFrame):
        return _inject_static_fields_df(data, static_fields)
    else:
        logger.debug(f"Data type: {type(data)}")
        logger.debug(f"Static fields: {static_fields}")
        logger.error("Data should be either a list of dictionaries or a pandas DataFrame.")
        raise ValueError("Data should be either a list of dictionaries or a pandas DataFrame.")

def _inject_static_fields_json(data: list[dict], static_fields: list[dict]) -> list[dict]:
    """ Inject static fields into a list of dictionaries. """
    for item in data:
        for static_field in static_fields:
            key = static_field.get("name")
            value = static_field.get("value")
            if key not in item:
                item[key] = value
            else:
                logger.warning(f"Static field {key} already exists in the data.")
    return data

def _inject_static_fields_df(data: pd.DataFrame, static_fields: list[dict]) -> pd.DataFrame:
    """ Inject static fields into a DataFrame. """
    for static_field in static_fields:
        key = static_field.get("name")
        value = static_field.get("value")
        if key not in data.columns:
            data[key] = value
        else:
            logger.warning(f"Static field {key} already exists in the data.")
    return data

def process_model(db_repo: Path, model: dict, manual: bool = False):
    """ TODO """
    model_name = model.get("name")
    model_trigger = model.get("trigger", "automatic")
    if not validate_trigger(model_trigger, manual):
        return
    logger.info(f"Processing model - {model_name}")
    
    # Output handler
    output = model.get("output", {})
    output_handler = FileHandlerFactory.create_file_handler(db_repo / output.get("path"))
    output_last_update = output_handler.last_update()

    # Source handlers
    sources = model.get("sources", [])
    loaded_sources = load_sources(sources, db_repo, version_threshold=output_last_update)
    logger.debug(f"Loaded sources: {loaded_sources.keys()}")

    # Processing
    processing_instructions = model.get("processing", {})
    processor = ProcessorFactory.get_processor(processing_instructions.pop("processor", None))
    data = processor.run(
        sources = loaded_sources,
        **processing_instructions
    )

    # Inject static fields
    static_fields = model.get("static_fields", {})
    if static_fields:
        data = inject_static_fields(data, static_fields)
        logger.debug(f"Injected static fields: {static_fields}")

    # Write Output
    output_handler.write(data, overwrite=output.get("overwrite", False))
    logger.info(f"Output written to: {output_handler.path}")

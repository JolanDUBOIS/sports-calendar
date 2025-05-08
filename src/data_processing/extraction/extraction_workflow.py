import json
from pathlib import Path
from abc import ABC, abstractmethod

import pandas as pd

from .extract_json import extract_from_json
from .extract_tables import extract_parallel_entities
from src.data_processing import logger
from src.data_processing.utils import order_models


class SourceLoader(ABC):
    @abstractmethod
    def load(self, path: str) -> pd.DataFrame:
        pass

class CSVLoader(SourceLoader):
    def load(self, path: str) -> pd.DataFrame:
        return pd.read_csv(path)

class JSONLoader(SourceLoader):
    def load(self, path: str) -> dict:
        with open(path, "r") as f:
            data = json.load(f)
        return data

LOADER_REGISTRY = {
    "csv": CSVLoader(),
    "json": JSONLoader(),
}


EXTRACTION_FUNCTION_REGISTRY = {
    "extract_from_json": extract_from_json,
    "extract_parallel_entities": extract_parallel_entities
}

def load_sources(db_repo: Path, sources: list[dict]) -> dict[str, pd.DataFrame|dict]:
    """ TODO """
    loaded_sources = {}
    for source in sources:
        source_name = source.get("name")
        source_type = source.get("type")
        source_path = source.get("path")
        
        loader = LOADER_REGISTRY.get(source_type)
        if not loader:
            logger.error(f"Unsupported source type: {source_type}.")
            raise ValueError(f"Unsupported source type: {source_type}.")
        
        loaded_sources[source_name] = loader.load(db_repo / source_path)

    return loaded_sources

def validate_table(data: pd.DataFrame, columns: list[dict]) -> pd.DataFrame:
    """ TODO """
    if columns is None:
        logger.error(f"Columns mapping is missing in instruction file.")
        raise ValueError(f"Columns mapping is missing in instruction file.")

    col_names = []
    for column in columns:
        if not isinstance(column, dict):
            logger.error(f"Column mapping should be a dictionary.")
            raise ValueError(f"Column mapping should be a dictionary.")
        col_name = column.get("name")
        if col_name not in data.columns:
            logger.warning(f"Column {col_name} is missing, creating empty column.")
            data[col_name] = None
        static_value = column.get("static")
        if static_value:
            data[col_name] = static_value
        col_names.append(col_name)

    if set(col_names) != set(data.columns):
        # TODO - Find better error msg
        logger.error(f"Output columns do not match the expected columns.")
        raise ValueError(f"Output columns do not match the expected columns.")

    return data

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
        model_trigger = model.get("trigger", "automatic")
        if not manual and model_trigger == "manual":
            continue
        if model_trigger != "automatic":
            logger.warning(f"Unsupported trigger type: {model_trigger}.")
            continue

        model_name = model.get("name")
        logger.info(f"Extraction for model: {model_name}")

        loaded_sources = load_sources(db_repo, model.get("sources", []))

        extraction_func = EXTRACTION_FUNCTION_REGISTRY.get(model.get("function"))
        if not extraction_func:
            logger.error(f"Unsupported extraction function: {model.get('function')}.")
            raise ValueError(f"Unsupported extraction function: {model.get('function')}.")
        list_params = model.get("params", [])

        output = pd.DataFrame()
        for params in list_params:
            new_output = extraction_func(sources = loaded_sources, **params)
            if not isinstance(output, pd.DataFrame):
                logger.error(f"Output from extraction function is not a DataFrame.")
                raise ValueError(f"Output from extraction function is not a DataFrame.")

            columns = model.get("columns")
            new_output = validate_table(new_output, columns)

            output = pd.concat([output, new_output], ignore_index=True)

        output_file = db_repo / model.get("output")
        save_file(output_file, output)
    logger.info("Extraction workflow completed.")

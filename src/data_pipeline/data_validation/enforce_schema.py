from pathlib import Path

import pandas as pd

from src.data_pipeline.data_validation import logger


def enforce_schema(db_layer_path: Path, model_schema: dict) -> tuple[str, Path, Exception | None]:
    """ Validate the given model schema. """
    model_name = model_schema.get("name")
    if not model_name:
        logger.error("Model name is missing in the schema.")
        raise ValueError("Model name is missing in the schema.")
    logger.info(f"Validating schema for model: {model_name}")

    try:
        model_path = db_layer_path / f"{model_name}.csv"
        if not model_path.exists():
            logger.error(f"Model path {model_path} does not exist.")
            raise FileNotFoundError(f"Model path {model_path} does not exist.")
        if not model_path.is_file() or not model_path.suffix == ".csv":
            logger.error(f"Model path {model_path} doesn't exist or is not a CSV file.")
            raise ValueError(f"Model path {model_path} doesn't exist or is not a CSV file.")
        data = pd.read_csv(model_path) # TODO - USE file_io !!! And filter with version field

        model_columns_schema = model_schema.get("columns")
        _check_column_schema(model_columns_schema)

        enforce_col_schema(data, model_columns_schema)
        logger.info(f"Data validation for {model_name} completed successfully.")
        return model_name, model_path, None
    except Exception as e:
        logger.error(f"Data validation failed for model {model_name}: {e}")
        return model_name, model_path, e

def enforce_col_schema(data: pd.DataFrame, col_schema: list[dict]) -> None:
    """ TODO """
    if not isinstance(data, pd.DataFrame):
        logger.error("Data must be a pandas DataFrame.")
        raise TypeError("Data must be a pandas DataFrame.")
    if data.empty:
        logger.debug("Data is empty.")
        return

    for col in col_schema:

        col_name = col.get("name")
        if col_name not in data.columns:
            logger.error(f"Column {col_name} is missing in the DataFrame.")
            raise ValueError(f"Column {col_name} is missing in the DataFrame.")

        col_type = col.get("type")
        if col_type:
            try:
                data[col_name] = data[col_name].astype(col_type)
            except Exception:
                logger.error(f"Column {col_name} type is invalid.")
                raise TypeError(f"Column {col_name} type is invalid.")

        col_nullable = col.get("nullable", True)
        if not col_nullable:
            if data[col_name].isnull().any():
                logger.error(f"Column {col_name} has null values.")
                raise ValueError(f"Column {col_name} has null values.")

        col_unique = col.get("unique")
        if col_unique:
            if not data[col_name].is_unique:
                logger.error(f"Column {col_name} is not unique.")
                raise ValueError(f"Column {col_name} is not unique.")

def _check_column_schema(col_schema: list[dict]) -> None:
    """ TODO """
    if not isinstance(col_schema, list):
        logger.debug(f"Columns schema: {col_schema}")
        logger.error("Columns schema must be a list.")
        raise TypeError("Columns schema must be a list.")

    if not all(isinstance(col, dict) for col in col_schema):
        logger.debug(f"Columns schema: {col_schema}")
        logger.error("All elements in the schema must be dictionaries.")
        raise TypeError("All elements in the schema must be dictionaries.")

    for col in col_schema:
        if "name" not in col:
            logger.debug(f"Columns schema: {col_schema}")
            logger.error("Column name is missing in the schema.")
            raise ValueError("Column name is missing in the schema.")

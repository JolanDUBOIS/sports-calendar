from pathlib import Path

import pandas as pd
from tabulate import tabulate

from . import logger
from .enforce_schema import enforce_schema
from src.data_pipeline.utils import read_yml_file


def validate_stage_schema(db_repo: str, schema_file_path: str, log_summary: bool = False) -> None:
    """ Validate one schema file corresponding to one stage. """
    schema_file_path = Path(schema_file_path).resolve()
    schemas = read_yml_file(schema_file_path)
    stage: str = schemas.get("stage")

    db_layer_path = Path(db_repo).resolve() / stage
    if not db_layer_path.exists():
        logger.error(f"Database repository path {db_layer_path} does not exist.")
        raise FileNotFoundError(f"Database repository path {db_layer_path} does not exist.")
    if not db_layer_path.is_dir():
        logger.error(f"Database repository path {db_layer_path} is not a directory.")
        raise NotADirectoryError(f"Database repository path {db_layer_path} is not a directory.")

    summary = {"stage": stage, "summary": {}}
    model_paths = []
    for model_schema in schemas.get("models", []):
        model_name, model_path, exception = enforce_schema(db_layer_path, model_schema)
        if model_name in summary:
            logger.error(f"Duplicate model name {model_name} found in schema.")
            raise ValueError(f"Duplicate model name {model_name} found in schema.")
        summary["summary"][model_name] = {
            "path": "/".join(model_path.parts[-4:]),
            "exception": type(exception).__name__ if exception else "Success",
            "exception msg": str(exception) if exception else None
        }
        model_paths.append(model_path.resolve())

    for item in db_layer_path.iterdir():
        if item.name == ".meta.json":
            continue
        if item.resolve() not in model_paths:
            logger.error(f"File {item} not in schema.")
            path = "/".join(item.resolve().parts[-4:])
            summary["summary"][f"_{item.stem}"] = {
                "path": path,
                "exception": "ValueError",
                "exception msg": f"File not in schema."
            }

    if log_summary:
        _log_summary(summary)
    logger.info(f"Data validation for stage {stage} completed.")

def _log_summary(summary: dict) -> None:
    """ Log the summary of data validation. """
    df = pd.DataFrame.from_dict(summary["summary"], orient="index")
    df = df.reset_index().rename(columns={"index": "name"})[["name", "exception", "exception msg", "path"]]
    logger.info(f"Data validation summary for stage {summary['stage']}:\n{tabulate(df[['name', 'exception', 'exception msg']], showindex=False, headers='keys')}")
    logger.debug(f"Detailed summary:\n{tabulate(df, showindex=False, headers='keys')}")

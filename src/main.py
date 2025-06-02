from __future__ import annotations
from typing import TYPE_CHECKING
from pathlib import Path

from src import logger
from .data_pipeline import LayerBuilder, LayerSchemaManager, DataStage

if TYPE_CHECKING:
    from .data_pipeline import SchemaValidationResult


ROOT_PATH = Path(__file__).parent.parent
CONFIG_PATH = ROOT_PATH / "config" / "pipeline_config"
DATA_PATH = ROOT_PATH / "data"

REPOSITORIES = {
    "test": "repository_test",
    "prod": "repository"
}

def run_pipeline_logic(
    repo: str = "test",
    stage: DataStage | None = None,
    model: str | None = None,
    manual: bool = False,
    dry_run: bool = False, # Not implemented yet
    verbose: bool = False # Not implemented yet
) -> None:
    """ TODO """
    config_folder = CONFIG_PATH / "workflows"
    repo_path = DATA_PATH / REPOSITORIES[repo]

    if model:
        if stage is None:
            logger.error("Stage must be specified when a model is provided.")
            raise ValueError("Stage must be specified when a model is provided.")
        yml_path = config_folder / f"build_{stage}.yml"
        LayerBuilder.from_yaml(yml_path, repo_path).build(manual=manual, model=model)
        return

    stages = [stage] if stage else DataStage.instances()
    for _stage in stages:
        logger.info(f"Running pipeline for stage: {_stage}")
        yml_path = config_folder / f"build_{_stage}.yml"
        if not yml_path.exists():
            logger.error(f"Configuration file not found: {yml_path}")
            raise FileNotFoundError(f"Configuration file not found: {yml_path}")
        else:
            LayerBuilder.from_yaml(yml_path, repo_path).build(manual=manual)

def run_validation_logic(
    repo: str = "test",
    stage: DataStage | None = None,
    model: str | None = None,
    raise_on_error: bool = False,
    verbose: bool = False # Not implemented yet
) -> list[SchemaValidationResult]:
    """ TODO """
    config_folder = CONFIG_PATH / "schemas"
    repo_path = DATA_PATH / REPOSITORIES[repo]

    if model:
        if stage is None:
            logger.error("Stage must be specified when a model is provided.")
            raise ValueError("Stage must be specified when a model is provided.")
        yml_path = config_folder / f"build_{stage}.yml"
        return [LayerSchemaManager.from_yaml(yml_path, repo_path).validate(raise_on_error=raise_on_error, model=model)]

    stages = [stage] if stage else DataStage.instances()
    results = []
    for _stage in stages:
        logger.info(f"Validating schema for stage: {_stage}")
        yml_path = config_folder / f"schema_{_stage}.yml"
        if not yml_path.exists():
            logger.error(f"Schema file not found: {yml_path}")
            raise FileNotFoundError(f"Schema file not found: {yml_path}")
        else:
            results.append(LayerSchemaManager.from_yaml(yml_path, repo_path).validate(raise_on_error=raise_on_error))

    return results

def run_selection_logic():
    """ TODO """
    raise NotImplementedError("Run selection is not implemented yet.")    

def run_test_logic(name: str):
    """ TODO """
    logger.info("No test currently implemented.")

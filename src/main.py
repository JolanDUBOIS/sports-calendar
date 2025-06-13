from __future__ import annotations
from typing import TYPE_CHECKING
from pathlib import Path

from src import logger
from .data_pipeline import PipelineConfig, DataStage, run_pipeline, run_validation

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
    dry_run: bool = False,
    verbose: bool = False # Not implemented yet
) -> None:
    """ TODO """
    pipeline_config = PipelineConfig(repo=repo)
    run_pipeline(
        pipeline_config=pipeline_config,
        stage=stage,
        model=model,
        manual=manual,
        dry_run=dry_run,
        verbose=verbose
    )

def run_validation_logic(
    repo: str = "test",
    stage: DataStage | None = None,
    model: str | None = None,
    raise_on_error: bool = False,
    verbose: bool = False # Not implemented yet
) -> list[SchemaValidationResult]:
    """ TODO """
    pipeline_config = PipelineConfig(repo=repo)
    return run_validation(
        pipeline_config=pipeline_config,
        stage=stage,
        model=model,
        raise_on_error=raise_on_error,
        verbose=verbose
    )

def run_selection_logic():
    """ TODO """
    raise NotImplementedError("Run selection is not implemented yet.")    

def run_test_logic(name: str):
    """ TODO """
    logger.info("No test currently implemented.")

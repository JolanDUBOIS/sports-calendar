from __future__ import annotations
from typing import TYPE_CHECKING

from src import logger
from .data_pipeline import PipelineConfig, run_pipeline, run_validation
from .fixture_pipeline import run_selection

if TYPE_CHECKING:
    from .data_pipeline import SchemaValidationResult


def run_pipeline_logic(
    repo: str = "test",
    **kwargs
) -> None:
    """ TODO """
    pipeline_config = PipelineConfig(repo=repo)
    run_pipeline(
        pipeline_config=pipeline_config,
        **kwargs
    )

def run_validation_logic(
    repo: str = "test",
    **kwargs
) -> list[SchemaValidationResult]:
    """ TODO """
    pipeline_config = PipelineConfig(repo=repo)
    return run_validation(
        pipeline_config=pipeline_config,
        **kwargs
    )

def run_selection_logic(**kwargs):
    """ TODO """
    run_selection(**kwargs)

def run_test_logic(name: str):
    """ TODO """
    logger.info("No test currently implemented.")

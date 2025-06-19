from __future__ import annotations
from typing import TYPE_CHECKING

from src import logger
from .data_pipeline import run_pipeline, run_validation
from .app import run_selection, clear_calendar

if TYPE_CHECKING:
    from .data_pipeline import SchemaValidationResult


def run_pipeline_logic(**kwargs) -> None:
    """ TODO """
    run_pipeline(**kwargs)

def run_validation_logic(**kwargs) -> list[SchemaValidationResult]:
    """ TODO """
    return run_validation(**kwargs)

def run_selection_logic(**kwargs):
    """ TODO """
    run_selection(**kwargs)

def clear_calendar_logic(**kwargs):
    """ TODO """
    clear_calendar(**kwargs)

def run_test_logic(**kwargs):
    """ TODO """
    logger.info("No test currently implemented.")

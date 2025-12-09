import pytest

from src.config import Config
from src.pipeline.processing.main import run_pipeline


def test_run_pipeline_single_model():
    config = Config()
    run_pipeline(config=config, stage="processing", model="model_a")

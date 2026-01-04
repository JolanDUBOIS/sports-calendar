from pathlib import Path

import pytest

from src.config import Config
from src.app import run_selection
from src.app.models import BaseTable


def test_run_selection():
    config = Config(repository="data/repository-test", environment="test")
    BaseTable.configure(Path("data/repository-test"))
    run_selection(config=config, key="test", dry_run=True)

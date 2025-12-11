from pathlib import Path

import pytest

from src.app import run_selection
from src.app.models import BaseTable


def test_run_selection():
    BaseTable.configure(Path("data/repository-test"))
    run_selection(key="test", dry_run=True)

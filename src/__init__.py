from src.config import setup_logging

setup_logging('config/logging_config.yml')

import logging

logger = logging.getLogger()

from pathlib import Path

ROOT_PATH = Path(__file__).parent.parent
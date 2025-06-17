from src.config import setup_logging

# 1. Setup logging
setup_logging('config/logging_config.yml')

# 2. Register custom YAML constructors
from .config import yaml_loader

# 3. Setup the default logger
import logging
logger = logging.getLogger()

# 4. Define root path
from pathlib import Path
ROOT_PATH = Path(__file__).parent.parent
# 1. Define root path
from pathlib import Path
ROOT_PATH = Path(__file__).parent.parent

# 2. Setup logging
from .config import setup_logging
setup_logging('config/logging_config.yml')

# 3. Register custom YAML constructors
from .utils import loader

# 4. Setup the default logger
import logging
logger = logging.getLogger()

import logging
from pathlib import Path

from sports_calendar.infra import Paths, setup_logging

logger = logging.getLogger(__name__)


def init_environment():
    """ Initialize the application environment by setting up paths and logging configuration. """
    # Initialize paths
    Paths.setup(app_name="sports-calendar")

    # Seed logging configuration if it doesn't exist
    Paths.seed_logging(Path(__file__).parent / "templates" / "logging.yml")

    # Setup logging
    setup_logging(config_file=Paths.LOG_CONFIG_FILE, log_dir=Paths.LOG_DIR)

    # Log the initialized paths for debugging
    Paths.log_paths(logger=logger)

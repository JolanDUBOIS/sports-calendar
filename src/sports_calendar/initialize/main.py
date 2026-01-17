import shutil
from pathlib import Path

from sports_calendar.core import Paths


def init():
    Paths.initialize()

    # Create the necessary directories if they don't exist
    Paths.CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    Paths.DB_DIR.mkdir(parents=True, exist_ok=True)
    Paths.LOG_DIR.mkdir(parents=True, exist_ok=True)
    Paths.CREDS_FOLDER.mkdir(parents=True, exist_ok=True)
    Paths.SECRETS_FOLDER.mkdir(parents=True, exist_ok=True)
    Paths.SELECTIONS_FOLDER.mkdir(parents=True, exist_ok=True)

    # Copy logging configuration template if it doesn't exist
    logging_config_template = Path(__file__).parent / "templates" / "logging.yml"
    logging_config_target = Paths.CONFIG_DIR / "logging.yml"
    if not logging_config_target.exists():
        shutil.copy(logging_config_template, logging_config_target)

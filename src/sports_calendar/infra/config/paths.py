import logging
import shutil
from pathlib import Path

from platformdirs import user_config_dir, user_state_dir

PROJECT_ROOT = Path(__file__).parent.parent.parent.parent.parent.resolve()
DEV_DEFAULT_CONFIG_DIR = PROJECT_ROOT / "config"
DEV_DEFAULT_LOG_DIR = PROJECT_ROOT / "logs"

class Paths:
    """ Manage application paths for config, data, and state. """
    CONFIG_DIR: Path
    LOG_DIR: Path

    LOG_CONFIG_FILE: Path
    CREDS_FOLDER: Path
    SECRETS_FOLDER: Path
    SELECTIONS_FOLDER: Path
    LABEL_CACHE_FILE: Path

    _setup: bool = False

    @classmethod
    def setup(cls, app_name: str = "sports-calendar"):
        cls.APP_NAME = app_name

        if DEV_DEFAULT_CONFIG_DIR.exists():
            cls.CONFIG_DIR = DEV_DEFAULT_CONFIG_DIR
        else:
            cls.CONFIG_DIR = Path(user_config_dir(cls.APP_NAME))

        if DEV_DEFAULT_LOG_DIR.exists():
            cls.LOG_DIR = DEV_DEFAULT_LOG_DIR
        else:
            cls.LOG_DIR = Path(user_state_dir(cls.APP_NAME)) / "logs"

        cls.LOG_CONFIG_FILE = cls.CONFIG_DIR / "logging.yml"
        cls.CREDS_FOLDER = cls.CONFIG_DIR / ".credentials"
        cls.SECRETS_FOLDER = cls.CONFIG_DIR / ".secrets"
        cls.SELECTIONS_FOLDER = cls.CONFIG_DIR / "selections"
        # State, not config: derived from the provider and safe to delete. It
        # lives beside the logs rather than in the config the user edits.
        cls.LABEL_CACHE_FILE = (
            Path(user_state_dir(cls.APP_NAME)) / "entity-labels.json"
        )

        cls._setup = True
        cls.create_folders()

    @classmethod
    def is_setup(cls) -> bool:
        return cls._setup

    @classmethod
    def create_folders(cls):
        if not cls._setup:
            raise RuntimeError("Paths not initialized")

        required_dirs = (
            cls.CONFIG_DIR,
            cls.LOG_DIR,
            cls.CREDS_FOLDER,
            cls.SECRETS_FOLDER,
            cls.SELECTIONS_FOLDER,
            cls.LABEL_CACHE_FILE.parent,
        )

        for directory in required_dirs:
            directory.mkdir(parents=True, exist_ok=True)

    @classmethod
    def seed_logging(cls, template_path: Path | str):
        if not cls._setup:
            raise RuntimeError("Paths not initialized")

        if not cls.LOG_CONFIG_FILE.exists():
            shutil.copy2(template_path, cls.LOG_CONFIG_FILE)

    @classmethod
    def log_paths(cls, logger: logging.Logger | None = None):
        """ Log all the resolved paths for debugging purposes. """
        if not cls._setup:
            raise RuntimeError("Paths not initialized")

        log = logger or logging.getLogger(__name__)

        log.debug("======= Current Paths =======")
        log.debug(f"APP_NAME        : {cls.APP_NAME}")
        log.debug(f"CONFIG_DIR      : {cls.CONFIG_DIR}")
        log.debug(f"LOG_DIR         : {cls.LOG_DIR}")
        log.debug(f"LOG_CONFIG_FILE : {cls.LOG_CONFIG_FILE}")
        log.debug(f"CREDS_FOLDER    : {cls.CREDS_FOLDER}")
        log.debug(f"SECRETS_FOLDER  : {cls.SECRETS_FOLDER}")
        log.debug(f"SELECTIONS_FOLDER: {cls.SELECTIONS_FOLDER}")
        log.debug("=============================")

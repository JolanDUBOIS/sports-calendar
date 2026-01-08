import os
import logging
from pathlib import Path
from platformdirs import user_config_dir, user_state_dir, user_data_dir


PROJECT_ROOT = Path(__file__).parent.parent.parent.parent.parent.resolve()
DEV_DEFAULT_DB_DIR = PROJECT_ROOT / "data"
DEV_DEFAULT_CONFIG_DIR = PROJECT_ROOT / "config"
DEV_DEFAULT_LOG_DIR = PROJECT_ROOT / "logs"

class Paths:
    """ Manage application paths for config, data, and state. """
    
    DB_DIR: Path

    CONFIG_DIR: Path
    LOG_DIR: Path

    LOG_CONFIG_FILE: Path
    CREDS_FOLDER: Path
    SECRETS_FOLDER: Path
    SELECTIONS_FOLDER: Path

    _initialized: bool = False

    @classmethod
    def initialize(cls, app_name: str = "sports-calendar"):
        cls.APP_NAME = app_name

        cls.DB_DIR = Path(os.getenv("DB_DIR")) if os.getenv("DB_DIR") else None
        if not cls.DB_DIR:
            if DEV_DEFAULT_DB_DIR.exists():
                cls.DB_DIR = DEV_DEFAULT_DB_DIR
            else:
                cls.DB_DIR = Path(user_data_dir(cls.APP_NAME))
        cls.DB_DIR.mkdir(parents=True, exist_ok=True)

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

        cls._initialized = True
        cls.validate()

    @classmethod
    def is_initialized(cls) -> bool:
        return cls._initialized

    @classmethod
    def validate(cls):
        if not cls._initialized:
            raise RuntimeError("Paths not initialized")
        if not cls.DB_DIR.exists():
            cls.DB_DIR.mkdir(parents=True, exist_ok=True)
        if not cls.CONFIG_DIR.exists():
            cls.CONFIG_DIR.mkdir(parents=True, exist_ok=True)

    @classmethod
    def log_paths(cls, logger: logging.Logger | None = None):
        """ Log all the resolved paths for debugging purposes. """
        if not cls._initialized:
            raise RuntimeError("Paths not initialized")

        log = logger or logging.getLogger(__name__)

        log.debug("==== Current Paths ====")
        log.debug(f"APP_NAME        : {cls.APP_NAME}")
        log.debug(f"DB_DIR          : {cls.DB_DIR}")
        log.debug(f"CONFIG_DIR      : {cls.CONFIG_DIR}")
        log.debug(f"LOG_DIR         : {cls.LOG_DIR}")
        log.debug(f"LOG_CONFIG_FILE : {cls.LOG_CONFIG_FILE}")
        log.debug(f"CREDS_FOLDER    : {cls.CREDS_FOLDER}")
        log.debug(f"SECRETS_FOLDER  : {cls.SECRETS_FOLDER}")
        log.debug(f"SELECTIONS_FOLDER: {cls.SELECTIONS_FOLDER}")
        log.debug("=======================")

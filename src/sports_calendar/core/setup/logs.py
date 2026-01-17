import logging.config
from pathlib import Path
from datetime import datetime

import yaml

from .. import logger


def setup_logging(config_file: Path, log_dir: Path) -> None:
    if not config_file.exists():
        raise FileNotFoundError(f"Logging configuration file {config_file} does not exist.")

    today_str = datetime.now().strftime("%Y-%m-%d")
    log_file = log_dir / f"sports-calendar-{today_str}.log"

    log_file.parent.mkdir(parents=True, exist_ok=True)

    with open(config_file, 'r') as f:
        config = yaml.safe_load(f.read())
    
    if "handlers" in config and "file_debug_handler" in config["handlers"]:
        config["handlers"]["file_debug_handler"]["filename"] = str(log_file)
    
    logging.config.dictConfig(config)

    now = datetime.now().strftime("%H:%M:%S")
    logger.debug("\n" + "="*80 + "\nStarting new logging session at " + now + "\n" + "="*80)
    logger.info(f"Logging initialized, logs will be written to {log_file}")

from pathlib import Path
from datetime import datetime, timedelta, timezone

import yaml

from . import logger


def date_offset_constructor(loader, node):
    """ Custom YAML constructor to handle date offsets. """
    days = int(node.value)
    return (datetime.now(timezone.utc) + timedelta(days=days)).isoformat(timespec="seconds")

yaml.add_constructor('!date_offset', date_offset_constructor, Loader=yaml.loader.SafeLoader)

def load_yml(path: str | Path) -> dict | list | None:
    """ Load a YAML file and return its content as a dictionary or list. """
    path = Path(path)
    if not path.exists():
        logger.error(f"YAML file {path} does not exist.")
        raise FileNotFoundError(f"YAML file {path} does not exist.")

    with open(path, 'r') as file:
        try:
            return yaml.safe_load(file)
        except yaml.YAMLError as e:
            logger.error(f"Error loading YAML file {path}: {e}")
            raise ValueError(f"Error loading YAML file {path}: {e}")

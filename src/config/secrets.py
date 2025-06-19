from pathlib import Path

from . import logger
from .loader import load_yml
from src import ROOT_PATH


SECRETS_DIR = ROOT_PATH / ".secrets"

class Secrets:

    def __init__(self):
        self.gcal_ids_path = SECRETS_DIR / "gcal_ids.yml"
        if not self.gcal_ids_path.exists():
            logger.error(f"Google Calendar IDs file not found: {self.gcal_ids_path}")
            raise FileNotFoundError(f"Google Calendar IDs file not found: {self.gcal_ids_path}")
        self.gcal_ids = load_yml(self.gcal_ids_path)

    def get_gcal_id(self, key: str) -> str:
        """ Get the Google Calendar ID for a given key. """
        if key not in self.gcal_ids:
            logger.error(f"Google Calendar ID for key '{key}' not found.")
            raise KeyError(f"Google Calendar ID for key '{key}' not found.")
        return self.gcal_ids[key]

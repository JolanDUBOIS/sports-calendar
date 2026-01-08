from pathlib import Path

from . import logger
from sports_calendar.sc_core import Paths


class Credentials:
    """ Configuration for Google API credentials. """

    def __init__(self):
        self.client_secret_path: Path = Paths.CREDS_FOLDER / "client_secret.json"
        self.token_path: Path = Paths.CREDS_FOLDER / "token.json"

        if not self.client_secret_path.exists():
            logger.error(f"Client secret file not found: {self.client_secret_path}")
            raise FileNotFoundError(f"Client secret file not found: {self.client_secret_path}")
        if not self.token_path.exists():
            logger.error(f"Token file not found: {self.token_path}")
            raise FileNotFoundError(f"Token file not found: {self.token_path}")

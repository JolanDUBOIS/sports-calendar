from dataclasses import dataclass
from pathlib import Path

from . import logger
from src import ROOT_PATH


CREDENTIALS_DIR = ROOT_PATH / ".credentials"

@dataclass
class Credentials:
    client_secret_path: Path = CREDENTIALS_DIR / "client_secret.json"
    token_path: Path = CREDENTIALS_DIR / "token.json"

    def __post_init__(self):
        if not self.client_secret_path.exists():
            logger.error(f"Client secret file not found: {self.client_secret_path}")
            raise FileNotFoundError(f"Client secret file not found: {self.client_secret_path}")
        if not self.token_path.exists():
            logger.error(f"Token file not found: {self.token_path}")
            raise FileNotFoundError(f"Token file not found: {self.token_path}")

from . import logger, CONFIG_DIR_PATH
from .loader import load_yml

class AppConfig:

    def __init__(self):
        self.config_path = CONFIG_DIR_PATH / "app"
        self._check_config_path()

        self.scopes = load_yml(self.config_path / "scopes.yml")

    def _check_config_path(self):
        """ Check if the app config directory exists and contains required files. """
        if not self.config_path.exists():
            logger.error(f"App config directory does not exist: {self.config_path}")
            raise FileNotFoundError(f"App config directory does not exist: {self.config_path}")

        required_files = ["scopes.yml"]
        for file in required_files:
            if not (self.config_path / file).exists():
                logger.error(f"Required file {file} does not exist in app config path.")
                raise FileNotFoundError(f"Required file {file} does not exist in app config path.")

    def get_scopes(self, key: str) -> any:
        """ TODO """
        return self.scopes.get(key)

    def get_gcal_delete_scope(self, key: str) -> dict:
        """ Get the Google Calendar delete scope for a given key. """
        delete_scope = self.get_scopes("gcal-delete", {})
        if key not in delete_scope:
            logger.error(f"Google Calendar delete scope for key '{key}' not found.")
            raise KeyError(f"Google Calendar delete scope for key '{key}' not found.")
        return delete_scope[key]

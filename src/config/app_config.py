import yaml
from pathlib import Path


class AppConfig:
    _instance = None

    def __new__(cls, config_path=Path("config/app_config.yml")):
        if cls._instance is None:
            cls._instance = super(AppConfig, cls).__new__(cls)
            cls._instance._load_config(config_path)
        return cls._instance

    def _load_config(self, path: Path):
        with path.open(mode="r") as f:
            self._config = yaml.safe_load(f)

    def get(self, key_path, default=None):
        keys = key_path.split(".")
        value = self._config
        try:
            for key in keys:
                value = value[key]
            return value
        except (KeyError, TypeError):
            return default

# Global accessor
def get_config(key_path, default=None):
    return AppConfig().get(key_path, default)

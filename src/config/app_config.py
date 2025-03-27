import yaml # type: ignore
from pathlib import Path

from src.config.utils import set_attribute


class AppConfig:
    """ Handles loading and accessing application configuration. """
    
    REQUIRED_KEYS = {
        'selection_file_path': {'type': Path},
        'google_credentials_file_path': {'type': Path},
        'database_path': {'type': Path}
    }
    
    def __init__(self, config_file: str):
        """ Initializes the AppConfig object. """        
        self.config_file = Path(config_file)
        self._load_config()
    
    def _load_config(self):
        """ Load the configuration file """
        if not self.config_file.exists():
            raise FileNotFoundError(f"Config file not found: {self.config_file}")

        with self.config_file.open(mode='r') as file:
            self.config = yaml.safe_load(file)
            if self.config is None:
                raise ValueError(f"Config file {self.config_file} is empty or malformed.")
        
        for key, key_info in self.REQUIRED_KEYS.items():
            value = self.config.get(key)
            if value is None:
                raise KeyError(f"Missing required key in config file: {key}")
            value_type = key_info.get('type', str)
            set_attribute(self, key.lower(), value, value_type)

    def __repr__(self):
        """ Represent the configuration object. """
        return f"AppConfig(config_file={self.config_file})"

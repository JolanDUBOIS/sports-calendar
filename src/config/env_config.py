import os 
from dotenv import load_dotenv # type: ignore

from src.config.utils import set_attribute


load_dotenv()

class EnvConfig:
    """ Class to handle .env configurations. """

    REQUIRED_KEYS = {
        'GOOGLE_CALENDAR_ID': {'type': str}
    }
    OPTIONAL_KEYS = {
        'FOOTBALL_DATA_API_TOKEN': {'type': str} # for deprecated functionalty
    }

    def __init__(self):
        """ Initializes the environment variables. """
        self._load_env()
    
    def _load_env(self):
        """ Load the environment variables. """
        for key, key_info in self.REQUIRED_KEYS.items():
            value = os.getenv(key)
            if value is None:
                raise KeyError(f"Missing required environment variable: {key}")
            value_type = key_info.get('type', str)
            set_attribute(self, key.lower(), value, value_type)
        
        for key, key_info in self.OPTIONAL_KEYS.items():
            value = os.getenv(key, None)
            value_type = key_info.get('type', str)
            set_attribute(self, key.lower(), value, value_type)

    def __repr__(self):
        """ Represent the configuration object. """
        return f"EnvConfig(REQUIRED_KEYS={self.REQUIRED_KEYS}, OPTIONAL_KEYS={self.OPTIONAL_KEYS})"

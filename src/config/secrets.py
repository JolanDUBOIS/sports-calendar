from pathlib import Path

import yaml

from . import logger


class SecretsManager:
    _secrets_dir = Path(".secrets")
    _loaded = False
    _data = {}

    _expected_files = {
        "gcal_ids.yml": {"type": dict, "key": "gcal_ids"}
    }

    @classmethod
    def load_all(cls):
        """ TODO """
        if cls._loaded:
            return
        for filename, specs in cls._expected_files.items():
            path = cls._secrets_dir / filename
            if not path.exists():
                logger.error(f"Secrets file '{filename}' not found in {cls._secrets_dir}")
                raise FileNotFoundError(f"Secrets file '{filename}' not found in {cls._secrets_dir}")
            with open(path, 'r') as f:
                data = yaml.safe_load(f)
                expected_type = specs["type"]
                if not isinstance(data, expected_type):
                    logger.error(f"Secrets file '{filename}' does not contain expected type {expected_type}")
                    raise TypeError(f"Secrets file '{filename}' does not contain expected type {expected_type}")
                cls._data[specs["key"]] = data
        cls._loaded = True

    @classmethod
    def get(cls, key: str, default=None):
        """ TODO """
        cls.load_all()
        return cls._data.get(key, default)

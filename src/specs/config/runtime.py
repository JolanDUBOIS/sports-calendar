from dataclasses import dataclass

from . import logger
from src.specs import BaseModel
from src.config.constants import ENV


@dataclass
class RuntimeConfig(BaseModel):
    repository: str
    environment: str

    def validate(self) -> None:
        """ Validate the runtime configuration. """
        if not self.environment in ENV:
            logger.error(f"Invalid environment '{self.environment}'. Must be one of: {ENV}")
            raise ValueError(f"Invalid environment '{self.environment}'. Must be one of: {ENV}")

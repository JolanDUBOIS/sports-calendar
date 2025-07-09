from pathlib import Path
from dataclasses import dataclass

from . import logger
from src.specs import BaseModel


@dataclass
class Repository(BaseModel):
    name: str
    path: Path

@dataclass
class InfrastructureConfig(BaseModel):
    repositories: list[Repository]

    def validate(self) -> None:
        """ Validate the infrastructure configuration. """
        if not all(isinstance(repo, Repository) for repo in self.repositories):
            logger.error("All items in 'repositories' must be of type 'Repository'.")
            raise ValueError("All items in 'repositories' must be of type 'Repository'.")
        names = [repo.name for repo in self.repositories]
        if len(names) != len(set(names)):
            logger.error("Repository names must be unique.")
            raise ValueError("Repository names must be unique.")

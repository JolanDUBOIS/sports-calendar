from pathlib import Path
from dataclasses import dataclass

from . import logger
from src.specs import BaseModel


@dataclass
class SourceVersioningStrategy(BaseModel):
    field: str
    mode: str

@dataclass
class SourceSpec(BaseModel):
    name: str
    path: Path
    versioning_strategy: SourceVersioningStrategy | None = None

    def resolve_path(self, base_path: Path | str) -> None:
        """ Resolve the path of the source relative to a base path. """
        base_path = Path(base_path)
        if not self.path.is_absolute():
            self.path = (base_path / self.path).resolve(strict=False)
            logger.debug(f"Path for source '{self.name}' resolved to: {self.path}")

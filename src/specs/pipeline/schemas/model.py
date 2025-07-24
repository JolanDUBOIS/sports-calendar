from pathlib import Path
from dataclasses import dataclass

from . import logger
from src.specs import BaseModel


@dataclass
class ColumnSpec(BaseModel):
    name: str
    type: str | None = None
    unique: bool = False
    nullable: bool = True
    enum: list[str] | None = None


@dataclass
class ModelSchemaSpec(BaseModel):
    name: str
    path: Path
    columns: list[ColumnSpec]

    def resolve_path(self, base_path: Path | str) -> None:
        """ Resolve the path of the model schema relative to a base path. """
        base_path = Path(base_path)
        if not self.path.is_absolute():
            self.path = (base_path / self.path).resolve(strict=False)
            logger.debug(f"Path for model '{self.name}' resolved to: {self.path}")

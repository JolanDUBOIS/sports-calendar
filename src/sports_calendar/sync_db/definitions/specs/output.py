from pathlib import Path
from typing import Union, Literal
from dataclasses import dataclass, field

from . import logger
from sports_calendar.core import SpecModel


# ---- Constraint Specs ----

@dataclass
class UniqueSpec(SpecModel):
    type: Literal["unique"]
    field_sets: list[list[str]]
    version_col: str
    keep: Literal["first", "last"] = "last"

@dataclass
class NonNullableSpec(SpecModel):
    type: Literal["non-nullable"]
    fields: list[str]

@dataclass
class CoerceSpec(SpecModel):
    type: Literal["coerce"]
    fields: list[str]
    cast_to: str

# ---- ConstraintSpec ----

ConstraintSpec = Union[UniqueSpec, NonNullableSpec, CoerceSpec]

# ---- Output spec ----

@dataclass
class OutputSpec(SpecModel):
    name: str
    path: Path
    layer: str
    constraints: list[ConstraintSpec] = field(default_factory=list)

    def resolve_path(self, base_path: Path | str) -> None:
        base_path = Path(base_path)
        if not self.path.is_absolute():
            self.path = (base_path / self.path).resolve(strict=False)
            logger.debug(f"Path for output '{self.name}' resolved to: {self.path}")

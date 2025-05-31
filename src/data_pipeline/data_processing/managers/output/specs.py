from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path

from . import logger


@dataclass
class UniqueSpec:
    field_sets: list[list[str]]
    version_col: str  # has to be a datetime field
    keep: str = "last"  # or "first"

    def __post_init__(self):
        """ Validate the uniqueness specification. """
        valid_keep_values = ["last", "first"]
        if self.keep not in valid_keep_values:
            logger.error(f"Invalid keep value '{self.keep}'. Valid values are: {valid_keep_values}.")
            raise ValueError(f"Invalid keep value '{self.keep}'. Valid values are: {valid_keep_values}.")

    def __repr__(self):
        """ String representation of the UniqueSpec. """
        return (
            f"UniqueSpec(field_sets={self.field_sets}, version_col={self.version_col}, "
            f"keep={self.keep})"
        )

    @classmethod
    def from_dict(cls, d: dict) -> UniqueSpec:
        """ Create a UniqueSpec from a dictionary. """
        return cls(
            field_sets=d["field_sets"],
            version_col=d["version_col"],
            keep=d.get("keep", "last")
        )

@dataclass
class NonNullableSpec:
    fields: list[str]

    def __repr__(self):
        """ String representation of the NonNullableSpec. """
        return f"NonNullableSpec(fields={self.fields})"

    @classmethod
    def from_dict(cls, d: dict) -> NonNullableSpec:
        """ Create a NonNullableSpec from a dictionary. """
        return cls(fields=d.get("fields", []))

@dataclass
class OutputSpec:
    name: str
    path: Path
    layer: str
    schema: str | None = None
    unique: UniqueSpec | None = None
    non_nullable: NonNullableSpec | None = None

    def __repr__(self):
        """ String representation of the OutputSpec. """
        return (
            f"OutputSpec(name={self.name}, path={self.path}, layer={self.layer}, "
            f"schema={self.schema}, unique={self.unique}, non_nullable={self.non_nullable})"
        )

    @classmethod
    def from_dict(cls, d: dict) -> OutputSpec:
        """ Create an OutputSpec from a dictionary. """
        return cls(
            name=d["name"],
            path=Path(d["path"]),
            layer=d["layer"],
            schema=d.get("schema"),
            unique=UniqueSpec.from_dict(d["unique"]) if "unique" in d else None,
            non_nullable=NonNullableSpec.from_dict(d["non-nullable"]) if "non-nullable" in d else None
        )

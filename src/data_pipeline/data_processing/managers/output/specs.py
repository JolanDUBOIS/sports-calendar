from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path

from . import logger


@dataclass
class UniqueSpec:
    """ TODO """
    fields: list[str]
    version_col: str  # has to be a datetime field
    keep: str = "last"  # or "first"

    def __post_init__(self):
        """ Validate the uniqueness specification. """
        valid_keep_values = ["last", "first"]
        if self.keep not in valid_keep_values:
            logger.error(f"Invalid keep value '{self.keep}'. Valid values are: {valid_keep_values}.")
            raise ValueError(f"Invalid keep value '{self.keep}'. Valid values are: {valid_keep_values}.")

    @classmethod
    def from_dict(cls, d: dict) -> UniqueSpec:
        """ Create a UniqueSpec from a dictionary. """
        return cls(
            fields=d["fields"],
            version_col=d["version_col"],
            keep=d.get("keep", "last")
        )

@dataclass
class NonNullableSpec:
    """ TODO """
    fields: list[str]

    @classmethod
    def from_dict(cls, d: dict) -> NonNullableSpec:
        """ Create a NonNullableSpec from a dictionary. """
        return cls(fields=d.get("fields", []))

@dataclass
class OutputSpec:
    """ Describes output file and expected schema, uniqueness, etc. """
    name: str
    path: Path
    layer: str
    schema: str | None = None
    unique: UniqueSpec | None = None
    non_nullable: NonNullableSpec | None = None

    @classmethod
    def from_dict(cls, d: dict) -> OutputSpec:
        """ Create an OutputSpec from a dictionary. """
        return cls(
            name=d["name"],
            path=Path(d["path"]),
            layer=d["layer"],
            schema=d.get("schema"),
            unique=UniqueSpec.from_dict(d["unique"]) if "unique" in d else None
        )

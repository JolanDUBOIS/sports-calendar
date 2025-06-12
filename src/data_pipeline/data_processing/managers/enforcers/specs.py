from __future__ import annotations
from abc import ABC, abstractmethod
from dataclasses import dataclass

from . import logger


class ConstraintSpec(ABC):
    """ Abstract base class for constraint specifications. """

    @classmethod
    @abstractmethod
    def from_dict(cls, d: dict) -> ConstraintSpec:
        """ Create a ConstraintSpec from a dictionary. """
        raise NotImplementedError("Subclasses must implement this method.")

@dataclass
class UniqueSpec(ConstraintSpec):
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
class NonNullableSpec(ConstraintSpec):
    fields: list[str]

    def __repr__(self):
        """ String representation of the NonNullableSpec. """
        return f"NonNullableSpec(fields={self.fields})"

    @classmethod
    def from_dict(cls, d: dict) -> NonNullableSpec:
        """ Create a NonNullableSpec from a dictionary. """
        return cls(fields=d.get("fields", []))

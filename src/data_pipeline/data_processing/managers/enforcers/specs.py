from __future__ import annotations
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from itertools import permutations
from typing import ClassVar

from . import logger


# Sub elements for the constraint specifications

@dataclass(frozen=True)
class EntitySpec:
    source: str
    source_id: str

    @classmethod
    def from_dict(cls, d: dict) -> EntitySpec:
        """ Create an EntitySpec from a dictionary. """
        return cls(
            source=d["source"],
            source_id=d["source_id"]
        )

@dataclass(frozen=True)
class EntitySpecs:
    """ A collection of EntitySpec objects. """
    entities: list[EntitySpec]

    def __iter__(self):
        """ Iterate over the EntitySpec objects in the collection. """
        return iter(self.entities)

    def __len__(self):
        """ Get the number of EntitySpec objects in the collection. """
        return len(self.entities)

    def all_pairs(self) -> list[tuple[EntitySpec, EntitySpec]]:
        """ Generate all pairs of entities from the collection (in both order). """
        return list(permutations(self.entities, 2))

    @classmethod
    def from_list(cls, l: list) -> EntitySpecs:
        """ Create an EntitySpecs from a list. """
        return cls([EntitySpec.from_dict(e) for e in l])


# Constraint specifications

class ConstraintSpec(ABC):
    """ Abstract base class for constraint specifications. """
    kind: ClassVar[str] = "base"
    _admin: bool = False  # Indicates if this is an admin rule spec

    @property
    def admin(self) -> bool:
        return self._admin

    @classmethod
    @abstractmethod
    def from_dict(cls, d: dict) -> ConstraintSpec:
        """ Create a ConstraintSpec from a dictionary. """
        raise NotImplementedError("Subclasses must implement this method.")

    @classmethod
    def from_kind(cls, kind: str) -> type["ConstraintSpec"]:
        for subclass in cls.__subclasses__():
            if getattr(subclass, "kind", None) == kind:
                return subclass
        raise ValueError(f"Unknown kind: {kind}")

@dataclass
class UniqueSpec(ConstraintSpec):
    kind: ClassVar[str] = "unique"
    field_sets: list[list[str]]
    version_col: str  # has to be a datetime field
    keep: str = "last"  # or "first"
    _admin: bool = False

    def __post_init__(self):
        valid_keep_values = ["last", "first"]
        if self.keep not in valid_keep_values:
            logger.error(f"Invalid keep value '{self.keep}'. Valid values are: {valid_keep_values}.")
            raise ValueError(f"Invalid keep value '{self.keep}'. Valid values are: {valid_keep_values}.")

    def __repr__(self):
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
    kind: ClassVar[str] = "non-nullable"
    fields: list[str]
    _admin: bool = False

    def __repr__(self):
        return f"NonNullableSpec(fields={self.fields})"

    @classmethod
    def from_dict(cls, d: dict) -> NonNullableSpec:
        """ Create a NonNullableSpec from a dictionary. """
        return cls(fields=d.get("fields", []))


# Admin contraint specifications

@dataclass
class AdminConstraintSpec(ConstraintSpec, ABC):
    kind: ClassVar[str] = "admin"
    entity_type: str

@dataclass
class MatchSpec(AdminConstraintSpec):
    kind: ClassVar[str] = "admin-match"
    entities: EntitySpecs
    _admin: bool = True

    def __post_init__(self):
        """ Validate the MatchSpec. """
        if not self.entities or len(self.entities) < 2:
            logger.error("MatchSpec requires at least two entities.")
            raise ValueError("MatchSpec requires at least two entities.")

    @classmethod
    def from_dict(cls, d: dict) -> MatchSpec:
        """ Create a MatchSpec from a dictionary. """
        return cls(
            entity_type=d["entity_type"],
            entities=EntitySpecs.from_list(d.get("entities", []))
        )

@dataclass
class ForceMatchSpec(MatchSpec):
    kind: ClassVar[str] = "force_match"

@dataclass
class BlockMatchSpec(MatchSpec):
    kind: ClassVar[str] = "block_match"


# Specifications collection

@dataclass
class ConstraintSpecs:
    specs: list[ConstraintSpec] = field(default_factory=list)

    def __post_init__(self):
        """ Validate the collection of constraint specifications. """
        if any(isinstance(spec, AdminConstraintSpec) for spec in self.specs):
            logger.error("AdminConstraintSpec cannot be part of a regular ConstraintSpecs collection.")
            raise ValueError("AdminConstraintSpec cannot be part of a regular ConstraintSpecs collection.")

    def __iter__(self):
        return iter(self.specs)

    def __len__(self):
        return len(self.specs)

    def __repr__(self):
        return ", ".join([repr(spec) for spec in self.specs])

    def kinds(self) -> set[str]:
        """ Get a set of all kinds of ConstraintSpecs in the collection. """
        return {spec.kind for spec in self.specs}

    def get(self, kind: str) -> ConstraintSpec:
        """ Get a ConstraintSpec by its kind. """
        for spec in self.specs:
            if spec.kind == kind:
                return spec
        logger.error(f"No ConstraintSpec of kind '{kind}' found in the collection.")
        raise ValueError(f"No ConstraintSpec of kind '{kind}' found in the collection.")

    @classmethod
    def from_dict(cls, d: dict) -> ConstraintSpecs:
        """ Create a ConstraintSpecs from a dictionary. """
        specs = []
        for key, item in d.items():
            try:
                spec_class = ConstraintSpec.from_kind(key)
                spec = spec_class.from_dict(item)
                specs.append(spec)
            except ValueError:
                pass
        return cls(specs=specs)            

@dataclass
class AdminConstraintSpecs:
    specs: list[AdminConstraintSpec] = field(default_factory=list)

    def __post_init__(self):
        """ Validate the collection of admin constraint specifications. """
        if not all(isinstance(spec, AdminConstraintSpec) for spec in self.specs):
            logger.error("All specs must be instances of AdminConstraintSpec.")
            raise TypeError("All specs must be instances of AdminConstraintSpec.")

    def __iter__(self):
        return iter(self.specs)

    def __len__(self):
        return len(self.specs)

    @classmethod
    def from_dict(cls, d: dict) -> AdminConstraintSpecs:
        """ Create an AdminConstraintSpecs from a dictionary. """
        specs = []
        for key, item in d.items():
            spec_class = ConstraintSpec.from_kind(key)
            spec = spec_class.from_dict(item)
            specs.append(spec)
        return cls(specs=specs)

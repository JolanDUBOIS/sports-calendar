from __future__ import annotations
from abc import ABC
from dataclasses import dataclass
from itertools import permutations
from pathlib import Path

import yaml

from . import logger
from .specs import ConstraintSpec


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
    def from_dict(cls, d: dict) -> EntitySpecs:
        """ Create an EntitySpecs from a dictionary. """
        entities = [EntitySpec.from_dict(entity) for entity in d.get("entities", [])]
        return cls(entities=entities)


@dataclass(frozen=True)
class AdminRuleSpec(ConstraintSpec, ABC):
    entity_type: str

@dataclass(frozen=True)
class ForceMatchSpec(AdminRuleSpec):
    entities: EntitySpecs

    @classmethod
    def from_dict(cls, d: dict) -> ForceMatchSpec:
        """ Create a ForceMatchSpec from a dictionary. """
        return cls(
            entity_type=d["entity_type"],
            entities=EntitySpecs.from_dict(d.get("entities", {}))
        )

@dataclass(frozen=True)
class BlockMatchSpec(AdminRuleSpec):
    entities: EntitySpecs

    @classmethod
    def from_dict(cls, d: dict) -> BlockMatchSpec:
        """ Create a BlockMatchSpec from a dictionary. """
        return cls(
            entity_type=d["entity_type"],
            entities=EntitySpecs.from_dict(d.get("entities", {}))
        )


class AdminRuleFactory:
    """ Factory class to create admin specs from dictionaries. """

    mapping = {
        "force_match": ForceMatchSpec,
        "block_match": BlockMatchSpec,
    }

    @classmethod
    def create_admin_spec(cls, d: dict) -> AdminRuleSpec:
        """ Create an admin rule spec from a dictionary. """
        rule_type = d.get("type")
        if rule_type not in cls.mapping:
            logger.error(f"Unknown admin rule type: {rule_type}")
            raise ValueError(f"Unknown admin rule type: {rule_type}")

        spec_class: AdminRuleSpec = cls.mapping[rule_type]
        return spec_class.from_dict(d)


def load_admin_specs_from_yaml(path: str | Path) -> list[AdminRuleSpec]:
    """ Load admin specs from a YAML file. """
    path = Path(path)
    if not path.exists():
        logger.error(f"Admin specs file not found: {path}")
        raise FileNotFoundError(f"Admin specs file not found: {path}")

    with path.open(mode="r") as f:
        try:
            data = yaml.safe_load(f)
            rules = data.get("rules", [])
            return [AdminRuleFactory.create_admin_spec(spec) for spec in rules]
        except yaml.YAMLError as e:
            logger.error(f"Error parsing YAML file: {e}")
            raise ValueError(f"Error parsing YAML file: {e}")

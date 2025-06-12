from __future__ import annotations
from pathlib import Path
from dataclasses import dataclass, field

import yaml

from . import logger


@dataclass
class CoalesceRuleSpec:
    col_name: str
    source_cols: list[str]
    nullable: bool = True

    def __repr__(self) -> str:
        return f"CoalesceRuleSpec(col_name={self.col_name}, source_cols={self.source_cols})"

    @classmethod
    def from_dict(cls, col_name: str, d: dict) -> CoalesceRuleSpec:
        """ Create a CoalesceRuleSpec from a dictionary. """
        return cls(
            col_name=col_name,
            source_cols=d["source_cols"],
            nullable=d.get("nullable", True)
        )

@dataclass
class CoalesceRuleSpecs:
    rules: list[CoalesceRuleSpec] = field(default_factory=dict)

    def __repr__(self):
        return f"CoalesceRuleSpecs(rules={self.rules})"

    def items(self):
        """ Get the items of the CoalesceRules. """
        return [(rule.col_name, rule.source_cols) for rule in self.rules]

    def get(self, col_name: str) -> CoalesceRuleSpec:
        """ Get a CoalesceRule by its column name. """
        for rule in self.rules:
            if rule.col_name == col_name:
                return rule
        logger.error(f"Coalesce rule for column '{col_name}' not found.")
        raise KeyError(f"Coalesce rule for column '{col_name}' not found.")

    def get_non_nullable_col_name(self) -> list[str]:
        """ Get the column names of non-nullable coalesce rules. """
        return [rule.col_name for rule in self.rules if not rule.nullable] 

    @classmethod
    def from_dict(cls, d: dict) -> CoalesceRuleSpecs:
        """ Create a CoalesceRules from a dictionary. """
        rules = []
        for col_name, rule_spec in d.items():
            rules.append(CoalesceRuleSpec.from_dict(col_name, rule_spec))
        return cls(rules=rules)

@dataclass
class RegistrySpec:
    entity_type: str
    canonical_mapping_table: str
    coalesce_rules: CoalesceRuleSpecs

    @classmethod
    def from_dict(cls, entity_type: str, d: dict) -> RegistrySpec:
        """ Create a RegistrySpec from a dictionary. """
        return cls(
            entity_type=entity_type,
            canonical_mapping_table=d["canonical_mapping_table"],
            coalesce_rules=CoalesceRuleSpecs.from_dict(d["coalesce_rules"])
        )

@dataclass
class RegistrySpecs:
    specs: list[RegistrySpec] = field(default_factory=list)

    def get(self, entity_type: str) -> RegistrySpec:
        """ Get a RegistrySpec by its entity type. """
        for spec in self.specs:
            if spec.entity_type == entity_type:
                return spec
        logger.error(f"Registry spec for entity type '{entity_type}' not found.")
        raise KeyError(f"Registry spec for entity type '{entity_type}' not found.")

    @classmethod
    def from_list(cls, specs: list[dict]) -> RegistrySpecs:
        """ Create a RegistrySpecs from a list of dictionaries. """
        if not isinstance(specs, list):
            logger.error("Invalid format for registry specs. Expected a list of dictionaries.")
            raise ValueError("Invalid format for registry specs. Expected a list of dictionaries.")
        
        return cls(
            specs=[RegistrySpec.from_dict(spec["entity_type"], spec) for spec in specs]
        )


def load_registry_specs(specs_path: str | Path) -> RegistrySpecs:
    specs_path = Path(specs_path)
    with specs_path.open(mode='r') as file:
        raw_data = yaml.safe_load(file)

    registry_specs = RegistrySpecs()

    for entity_type, specs in raw_data.items():
        if not isinstance(specs, dict):
            logger.error(f"Invalid format for entity type '{entity_type}' in YAML file. Expected a dictionary.")
            raise ValueError(f"Invalid format for entity type '{entity_type}' in YAML file. Expected a dictionary.")
        
        registry_specs.specs.append(RegistrySpec.from_dict(entity_type, specs))

    return registry_specs

REGISTRY_SPECS = load_registry_specs(Path(__file__).parent / "config.yml")

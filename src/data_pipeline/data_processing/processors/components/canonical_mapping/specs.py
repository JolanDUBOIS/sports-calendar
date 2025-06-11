from __future__ import annotations
from pathlib import Path
from dataclasses import dataclass, field

import yaml

from . import logger


@dataclass
class SourceEntityTableSpec:
    name: str
    id_col: str

    @classmethod
    def from_dict(cls, d: dict) -> SourceEntityTableSpec:
        """ Create a SourceEntityTableSpec from a dictionary. """
        return cls(
            name=d["name"],
            id_col=d["id_col"]
        )

@dataclass
class SourceEntityTableSpecs:
    specs: list[SourceEntityTableSpec] = field(default_factory=list)

    def get(self, name: str) -> SourceEntityTableSpec:
        """ Get a SourceEntityTableSpec by its name. """
        for spec in self.specs:
            if spec.name == name:
                return spec
        logger.error(f"Source entity table spec '{name}' not found.")
        raise KeyError(f"Source entity table spec '{name}' not found.")

    @classmethod
    def from_list(cls, specs: list[dict]) -> SourceEntityTableSpecs:
        """ Create a SourceEntityTableSpecs from a list of dictionaries. """
        if not isinstance(specs, list):
            logger.error("Invalid format for source entity table specs. Expected a list of dictionaries.")
            raise ValueError("Invalid format for source entity table specs. Expected a list of dictionaries.")
        
        return cls(
            specs=[SourceEntityTableSpec.from_dict(spec) for spec in specs]
        )

@dataclass
class CoalesceRule:
    col_name: str
    source_cols: list[str]

@dataclass
class CoalesceRules:
    rules: list[CoalesceRule] = field(default_factory=dict)

    def items(self):
        """ Get the items of the CoalesceRules. """
        return [(rule.col_name, rule.source_cols) for rule in self.rules]

    def get(self, col_name: str) -> CoalesceRule:
        """ Get a CoalesceRule by its column name. """
        for rule in self.rules:
            if rule.col_name == col_name:
                return rule
        logger.error(f"Coalesce rule for column '{col_name}' not found.")
        raise KeyError(f"Coalesce rule for column '{col_name}' not found.")

    @classmethod
    def from_dict(cls, d: dict) -> CoalesceRules:
        """ Create a CoalesceRules from a dictionary. """
        rules = []
        for col_name, source_cols in d.items():
            if not isinstance(source_cols, list):
                logger.error(f"Invalid format for coalesce rule '{col_name}'. Expected a list of source columns.")
                raise ValueError(f"Invalid format for coalesce rule '{col_name}'. Expected a list of source columns.")
            rules.append(CoalesceRule(col_name=col_name, source_cols=source_cols))
        return cls(rules=rules)

@dataclass
class CanonicalMappingSpec:
    entity_type: str
    similarity_table: str
    source_entity_tables: SourceEntityTableSpecs
    coalesce_rules: dict[str, list[str]]

    @classmethod
    def from_dict(cls, entity_type: str, d: dict) -> CanonicalMappingSpec:
        """ Create a CanonicalMappingSpec from a dictionary. """
        return cls(
            entity_type=entity_type,
            similarity_table=d["similarity_table"],
            source_entity_tables=SourceEntityTableSpecs.from_list(d["source_entity_tables"]),
            coalesce_rules=d.get("coalesce_rules", {})
        )

@dataclass
class CanonicalMappingSpecs:
    specs: list[CanonicalMappingSpec] = field(default_factory=list)

    def get(self, entity_type: str) -> CanonicalMappingSpec:
        """ Get a CanonicalMappingSpec by its entity type. """
        for spec in self.specs:
            if spec.entity_type == entity_type:
                return spec
        logger.error(f"Canonical mapping spec for entity type '{entity_type}' not found.")
        raise KeyError(f"Canonical mapping spec for entity type '{entity_type}' not found.")

    def append(self, spec: CanonicalMappingSpec) -> None:
        """ Add a new CanonicalMappingSpec to the collection. """
        if any(existing_spec.entity_type == spec.entity_type for existing_spec in self.specs):
            logger.error(f"Canonical mapping spec for entity type '{spec.entity_type}' already exists.")
            raise ValueError(f"Canonical mapping spec for entity type '{spec.entity_type}' already exists.")
        self.specs.append(spec)


def load_canonical_mapping_specs(specs_path: str | Path) -> CanonicalMappingSpecs:
    specs_path = Path(specs_path)
    with specs_path.open(mode='r') as f:
        raw_data = yaml.safe_load(f)
    
    canonical_mapping_specs = CanonicalMappingSpecs()

    for entity_type, specs in raw_data.items():
        if not isinstance(specs, dict):
            logger.error(f"Invalid format for entity type '{entity_type}' in YAML file. Expected a dictionary.")
            raise ValueError(f"Invalid format for entity type '{entity_type}' in YAML file. Expected a dictionary.")

        canonical_mapping_specs.append(CanonicalMappingSpec.from_dict(
            entity_type=entity_type,
            d=specs
        ))

    return canonical_mapping_specs

CANONICAL_MAPPING_SPECS = load_canonical_mapping_specs(
    Path(__file__).parent / "config.yml"
)

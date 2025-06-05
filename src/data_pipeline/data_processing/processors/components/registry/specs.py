from __future__ import annotations
from dataclasses import dataclass, field
from pathlib import Path

import yaml

from . import logger


@dataclass
class SourceTableSpec:
    table_name: str # e.g. espn_competitions, football_data_areas
    id_col: str
    match_columns: list[str]
    source_name: str | None = None # e.g. espn, football-data, livesoccertv

    def __post_init__(self):
        """ Validate the SourceTableSpec after initialization. """
        if not self.table_name or not self.id_col or not self.match_columns:
            logger.error("SourceTableSpec must have a table_name, id_col, and match_columns.")
            raise ValueError("SourceTableSpec must have a table_name, id_col, and match_columns.")

    @classmethod
    def from_dict(cls, d: dict) -> SourceTableSpec:
        """ Create a SourceTableSpec from a dictionary. """
        return cls(
            table_name=d["table_name"],
            id_col=d["id_col"],
            match_columns=d["match_columns"],
            source_name=d.get("source_name")
        )

@dataclass
class RegistrySpec:
    entity_type: str # e.g. competitions, areas, teams
    sources: list[SourceTableSpec]
    generic_tokens: list[str] = field(default_factory=list) # e.g. ["fc", "atletic club"]

    def get(self, source_name: str) -> SourceTableSpec:
        """ Get a SourceTableSpec by its source name. """
        for source in self.sources:
            if source.table_name == source_name:
                return source
        logger.error(f"Table '{source_name}' not found in registry spec for entity type '{self.entity_type}'.")
        raise KeyError(f"Table '{source_name}' not found in registry spec for entity type '{self.entity_type}'.")

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

    def append(self, spec: RegistrySpec):
        """ Add a new RegistrySpec to the collection. """
        if any(existing_spec.entity_type == spec.entity_type for existing_spec in self.specs):
            logger.error(f"Registry spec for entity type '{spec.entity_type}' already exists.")
            raise ValueError(f"Registry spec for entity type '{spec.entity_type}' already exists.")
        self.specs.append(spec)

@dataclass
class ReferenceEntitySpec:
    entity_type: str
    coalesce_rules: dict[str, list[str]] # e.g., "name": ["espn_teams.name", "football_data_teams.name"]

@dataclass
class ReferenceEntitySpecs:
    specs: list[ReferenceEntitySpec] = field(default_factory=list)

    def get(self, entity_type: str) -> ReferenceEntitySpec:
        """ Get a ReferenceEntitySpec by its entity type. """
        for spec in self.specs:
            if spec.entity_type == entity_type:
                return spec
        logger.error(f"Reference entity spec for entity type '{entity_type}' not found.")
        raise KeyError(f"Reference entity spec for entity type '{entity_type}' not found.")

    def append(self, spec: ReferenceEntitySpec):
        """ Add a new ReferenceEntitySpec to the collection. """
        if any(existing_spec.entity_type == spec.entity_type for existing_spec in self.specs):
            logger.error(f"Reference entity spec for entity type '{spec.entity_type}' already exists.")
            raise ValueError(f"Reference entity spec for entity type '{spec.entity_type}' already exists.")
        self.specs.append(spec)


def load_entity_specs(path: str | Path) -> tuple[RegistrySpecs, ReferenceEntitySpecs]:
    with open(path, 'r') as f:
        raw_data = yaml.safe_load(f)

    registry_specs = RegistrySpecs()
    reference_specs = ReferenceEntitySpecs()

    for entity_type, specs in raw_data.items():
        if not isinstance(specs, dict):
            logger.error(f"Invalid format for entity type '{entity_type}' in YAML file. Expected a dictionary.")
            raise ValueError(f"Invalid format for entity type '{entity_type}' in YAML file. Expected a dictionary.")

        sources = [
            SourceTableSpec.from_dict(source) for source in specs.get("sources", [])
        ]
        registry_specs.append(RegistrySpec(
            entity_type=entity_type,
            sources=sources,
            **specs.get("registry", {})
        ))
        reference_specs.append(ReferenceEntitySpec(
            entity_type=entity_type,
            coalesce_rules=specs.get("reference", {}).get("coalesce_rules", {})
        ))
    
    return registry_specs, reference_specs

REGISTRY_SPECS, REFERENCE_SPECS = load_entity_specs(
    Path(__file__).parent / "entity_resolution_config.yml"
)

from __future__ import annotations
from pathlib import Path
from dataclasses import dataclass, field

import yaml

from . import logger


@dataclass
class SourceTableSpec:
    table_name: str
    id_col: str

    @classmethod
    def from_dict(cls, d: dict) -> SourceTableSpec:
        """ Create a SourceTableSpec from a dictionary. """
        if 'table_name' not in d or 'id_col' not in d:
            logger.error("SourceTableSpec must contain 'table_name' and 'id_col'.")
            raise KeyError("SourceTableSpec must contain 'table_name' and 'id_col'.")
        return cls(table_name=d['table_name'], id_col=d['id_col'])

@dataclass
class EntitySpec:
    entity_type: str
    similarity_table: str
    source_tables: list[SourceTableSpec]

    @property
    def table_names(self) -> list[str]:
        """ Get the table names for all source tables in this entity spec. """
        return [source.table_name for source in self.source_tables]

    @property
    def id_cols(self) -> list[str]:
        """ Get the ID columns for all source tables in this entity spec. """
        return [source.id_col for source in self.source_tables]

    def get(self, table_name: str) -> SourceTableSpec:
        """ Get the source table specification by table name. """
        for source in self.source_tables:
            if source.table_name == table_name:
                return source
        logger.error(f"Table '{table_name}' not found in entity spec for entity type '{self.entity_type}'.")
        raise KeyError(f"Table '{table_name}' not found in entity spec for entity type '{self.entity_type}'.")

    @classmethod
    def from_dict(cls, entity_type: str, d: dict) -> EntitySpec:
        """ Create an EntitySpec from a list of source table dictionaries. """
        return cls(
            entity_type=entity_type,
            similarity_table=d["similarity_table"],
            source_tables=[
                SourceTableSpec.from_dict(source) for source in d["source_tables"]
            ]
        )

@dataclass
class CanonicalMappingSpecs:
    entity_specs: list[EntitySpec] = field(default_factory=list)

    def get(self, entity_type: str) -> EntitySpec:
        """ Get an EntitySpec by its entity type. """
        for spec in self.entity_specs:
            if spec.entity_type == entity_type:
                return spec
        logger.error(f"Entity spec for entity type '{entity_type}' not found.")
        raise KeyError(f"Entity spec for entity type '{entity_type}' not found.")

    def append(self, entity_spec: EntitySpec) -> None:
        """ Append an EntitySpec to the list of entity specifications. """
        self.entity_specs.append(entity_spec)

    @classmethod
    def from_dict(cls, d: dict) -> CanonicalMappingSpecs:
        """ Create a CanonicalMappingSpecs from a dictionary. """
        canonical_mapping_specs = cls()
        for key, value in d.items():
            canonical_mapping_specs.append(EntitySpec.from_dict(key, value))
        return canonical_mapping_specs

def load_canonical_mapping_specs(path: str | Path) -> CanonicalMappingSpecs:
    """ Load canonical mapping specifications from a YAML file. """
    path = Path(path)
    with path.open(mode='r') as f:
        raw_data = yaml.safe_load(f)

    if not isinstance(raw_data, dict):
        logger.error("Canonical mapping specs must be a dictionary.")
        raise ValueError("Canonical mapping specs must be a dictionary.")

    return CanonicalMappingSpecs.from_dict(raw_data)

CANONICAL_MAPPING_SPECS = load_canonical_mapping_specs(
    Path(__file__).parent / "config.yml"
)

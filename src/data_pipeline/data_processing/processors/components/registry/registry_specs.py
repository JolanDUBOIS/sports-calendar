from __future__ import annotations
from pathlib import Path
from dataclasses import dataclass, field

import yaml

from . import logger


@dataclass
class SourceRegistrySpec:
    id_col: str
    column_variants: list[str] = field(default_factory=list)

    @classmethod
    def from_dict(cls, d: dict) -> SourceRegistrySpec:
        """ Create a SourceRegistrySpec from a dictionary. """
        return cls(
            id_col=d["id_col"],
            column_variants=d["column_variants"]
        )

@dataclass
class RegistrySpec:
    registry_key: str
    generic_tokens: list[str] = field(default_factory=list)
    registry_parameters: dict[str, SourceRegistrySpec] = field(default_factory=dict)

    def get_source_spec(self, source_key: str) -> SourceRegistrySpec:
        """ Get the registry parameters for a specific source key. """
        if source_key not in self.registry_parameters:
            logger.error(f"Source key '{source_key}' not found in registry parameters.")
            raise KeyError(f"Source key '{source_key}' not found in registry parameters.")
        return self.registry_parameters[source_key]

    @classmethod
    def from_dict(cls, key: str, d: dict) -> RegistrySpec:
        """ Create a RegistrySpec from a dictionary. """
        return cls(
            registry_key=key,
            generic_tokens=d.get("generic_tokens", []),
            registry_parameters={
                key: SourceRegistrySpec.from_dict(value)
                for key, value in d.get("registry_parameters", {}).items()
            }
        )

@dataclass
class RegistrySpecs:
    specs: dict[str, RegistrySpec] = field(default_factory=dict)

    def get(self, key: str) -> RegistrySpec:
        """ Get a RegistrySpec by its key. """
        if key not in self.specs:
            logger.error(f"Registry spec '{key}' not found.")
            raise KeyError(f"Registry spec '{key}' not found.")
        return self.specs[key]

    def get_source_spec(self, registry_key: str, source_key: str) -> SourceRegistrySpec:
        """ Get the source registry spec for a specific source key across all specs. """
        if registry_key not in self.specs:
            logger.error(f"Registry spec '{registry_key}' not found.")
            raise KeyError(f"Registry spec '{registry_key}' not found.")
        return self.specs[registry_key].get_source_spec(source_key)

    @classmethod
    def from_dict(cls, d: dict) -> RegistrySpecs:
        """ Create a RegistrySpecs from a dictionary. """
        specs = {key: RegistrySpec.from_dict(key, value) for key, value in d.items()}
        return cls(specs=specs)

    @classmethod
    def from_yaml(cls, path: Path) -> RegistrySpecs:
        """ Load registry specs from a YAML file. """
        with path.open("r", encoding="utf-8") as file:
            data = yaml.safe_load(file)
        if not isinstance(data, dict):
            logger.error(f"Invalid format in YAML file {path}. Expected a dictionary.")
            raise ValueError(f"Invalid format in YAML file {path}. Expected a dictionary.")
        return cls.from_dict(data)

REGISTRY_SPECS = RegistrySpecs.from_yaml(Path(__file__).parent / "constants.yml")

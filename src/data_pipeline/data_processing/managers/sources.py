from __future__ import annotations
from typing import TYPE_CHECKING
from dataclasses import dataclass
from pathlib import Path

from ..versioning import SourceVersioningStrategy, SourceVersions, SourceVersion, version_filter
from ...utils import get_max_field_value
from ...file_io import FileHandlerFactory

if TYPE_CHECKING:
    from ...types import IOContent


@dataclass
class SourceSpec:
    """Describes a single source input (path, name, versioning info)."""
    name: str
    path: Path
    versioning_strategy: SourceVersioningStrategy | None = None

    @classmethod
    def from_dict(cls, d: dict) -> SourceSpec:
        """ Create a SourceSpec from a dictionary. """
        return cls(
            name=d["name"],
            path=Path(d["path"]),
            versioning_strategy=SourceVersioningStrategy.from_dict(d["versioning"]) if "versioning" in d else None
        )

class SourcesManager:
    """ TODO """

    def __init__(self, sources: list[SourceSpec]):
        """ TODO """
        self.sources = sources

    def get_loaded_sources(self, source_versions: SourceVersions) -> dict[str, IOContent]:
        """ TODO """
        loaded_sources = {}
        for source in self.sources:
            source_version = source_versions.get(source.name)
            loaded_sources[source.name] = self._load_source_data(source, source_version)
        return loaded_sources

    def get_new_source_versions(self) -> SourceVersions:
        """ TODO """        
        source_versions = SourceVersions()
        for source in self.sources:
            if source.versioning_strategy:
                file_handler = FileHandlerFactory.create_file_handler(source.path, tracked=True)
                data = file_handler.read()
                cutoff = get_max_field_value(data, source.versioning_strategy.field)
                source_versions.append(
                    key=source.name,
                    version=SourceVersion(
                        version_field=source.versioning_strategy.field,
                        version_cutoff=cutoff
                    )
                )
        return source_versions

    @staticmethod
    def _load_source_data(source: SourceSpec, source_version: SourceVersion | None = None) -> IOContent:
        """ TODO """
        file_handler = FileHandlerFactory.create_file_handler(source.path, tracked=True)
        data = file_handler.read()
        data = version_filter(
            data=data,
            strategy=source.versioning_strategy,
            source_versions=source_version
        )
        return data

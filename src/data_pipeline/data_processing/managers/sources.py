from __future__ import annotations
from typing import TYPE_CHECKING

from . import logger
from ..versioning import SourceVersions, SourceVersion, version_filter
from ...utils import get_max_field_value
from ....file_io import FileHandlerFactory

if TYPE_CHECKING:
    from src.types import IOContent
    from src.specs import SourceSpec


class SourcesManager:
    """ Manage the loading and versioning of source data for model processing. """

    def __init__(self, sources: list[SourceSpec]):
        """ Initialize with a list of source specifications. """
        self.sources = sources

    def get_loaded_sources(self, source_versions: SourceVersions | None = None) -> dict[str, IOContent]:
        """ Load and return source data, filtered by provided version information. """
        loaded_sources = {}
        for source in self.sources:
            source_version = source_versions.get(source.name) if source_versions else None
            loaded_sources[source.name] = self._load_source_data(source, source_version)
        return loaded_sources

    def get_new_source_versions(self) -> SourceVersions:
        """ Determine and return the latest version cutoff for each versioned source. """        
        source_versions = SourceVersions()
        for source in self.sources:
            if source.versioning_strategy:
                file_handler = FileHandlerFactory.create_file_handler(source.path)
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
        """ Read and return data from a source, optionally filtering by version. """
        file_handler = FileHandlerFactory.create_file_handler(source.path)
        data = file_handler.read()
        data = version_filter(
            data=data,
            strategy=source.versioning_strategy,
            source_version=source_version
        )
        return data

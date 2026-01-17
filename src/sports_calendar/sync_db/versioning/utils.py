from __future__ import annotations
from typing import TYPE_CHECKING

from . import SourceVersions, SourceVersion, logger
from sports_calendar.sync_db.utils import filter_file_content

if TYPE_CHECKING:
    from . import SourceVersioningStrategy
    from sports_calendar.core import IOContent
    from sports_calendar.core.file_io import MetadataEntry


def read_versions(metadata_entry: MetadataEntry) -> SourceVersions:
    """ Read source versions from metadata entry. """
    logger.debug(f"Reading source versions from metadata entry: {metadata_entry}.")
    versions = SourceVersions()
    if metadata_entry.source_versions:
        for source_name, version in metadata_entry.source_versions.items():
            version_field = version['version_field']
            version_cutoff = version['version_cutoff']
            if version_field is not None and version_cutoff is not None:
                versions.append(
                    key=source_name,
                    version=SourceVersion(
                        version_field=version_field,
                        version_cutoff=version_cutoff
                    )
                )
            else:
                logger.debug(f"Empty version for source '{source_name}' in metadata entry, skipping.")
    return versions

def version_filter(
    data: IOContent,
    strategy: SourceVersioningStrategy | None = None,
    source_version: SourceVersion | None = None
) -> IOContent:
    """ Apply a filter on the data based on the source versioning strategy and source versions. """
    if strategy is None or source_version is None:
        logger.debug("Skipping version filter as no strategy or source versions provided.")
        return data

    op = strategy.get_operator()
    if op is None:
        return data
    else:
        if strategy.field != source_version.version_field:
            logger.warning(f"Version field mismatch: strategy field '{strategy.field}' does not match source version field '{source_version.version_field}'. Skipping filter.")
            return data

        return filter_file_content(
            data=data,
            field=strategy.field,
            op=op,
            value=source_version.version_cutoff
        )

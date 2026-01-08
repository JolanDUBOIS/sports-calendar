from typing import Any
from dataclasses import dataclass, field

from . import logger


@dataclass
class SourceVersion:
    """ Stores last seen/processed version info per source. """
    version_field: str  # has to be a datetime field
    version_cutoff: str | None = None  # the last seen/processed version cutoff

    def __post_init__(self) -> None:
        """ Validate version_field and version_cutoff. """
        if not self.version_field:
            logger.debug(f"Invalid SourceVersion initialization with field: {self.version_field}, cutoff: {self.version_cutoff}")
            logger.error("version_field must be provided.")
            raise ValueError("version_field must be provided.")
        if not isinstance(self.version_field, str):
            logger.debug(f"Invalid SourceVersion initialization with field: {self.version_field}, cutoff: {self.version_cutoff}")
            logger.error("version_field must be a string.")
            raise TypeError("version_field must be a string.")

    def to_dict(self) -> dict:
        """ Convert SourceVersion to a dictionary. """
        return {
            "version_field": self.version_field,
            "version_cutoff": self.version_cutoff
        }

@dataclass
class SourceVersions:
    """ Container for multiple SourceVersion objects keyed by source name. """
    source_versions: dict[str, SourceVersion] = field(default_factory=dict)

    def get(self, key: str, default: Any = None) -> str | None:
        """ Get the SourceVersion for a given source name. """
        return self.source_versions.get(key, default)

    def append(self, key: str, version: SourceVersion) -> None:
        """ Append a new SourceVersion for a given source name. """
        if key in self.source_versions:
            logger.error(f"Source '{key}' already exists in source_versions.")
            raise KeyError(f"Source '{key}' already exists in source_versions.")
        self.source_versions[key] = version

    def to_dict(self) -> dict[str, dict]:
        """ Convert SourceVersions to a dictionary. """
        return {key: version.to_dict() for key, version in self.source_versions.items()}

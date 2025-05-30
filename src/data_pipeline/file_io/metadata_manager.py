from __future__ import annotations
import json
from datetime import datetime
from pathlib import Path
from dataclasses import dataclass

from . import logger


@dataclass
class MetadataEntry:
    """ Represents a single metadata entry. """
    timestamp: str
    operation: str
    added: int = 0
    removed: int = 0
    version: int = 1
    source_versions: dict | None = None

    def __repr__(self):
        """ Return a string representation of the metadata entry. """
        return (
            f"MetadataEntry(timestamp={self.timestamp}, operation={self.operation}, "
            f"added={self.added}, removed={self.removed}, version={self.version}, "
            f"source_versions={self.source_versions})"
        )

    def to_dict(self) -> dict:
        """ Convert the entry to a dictionary for JSON serialization. """
        return {
            "timestamp": self.timestamp,
            "operation": self.operation,
            "added": self.added,
            "removed": self.removed,
            "version": self.version,
            "source_versions": self.source_versions
        }

    @classmethod
    def from_dict(cls, d: dict) -> MetadataEntry:
        """ Create a MetadataEntry from a dictionary. """
        return cls(
            timestamp=d.get("timestamp"),
            operation=d.get("operation"),
            added=d.get("added", 0),
            removed=d.get("removed", 0),
            version=d.get("version", 1),
            source_versions=d.get("source_versions")
        )


class MetadataManager:
    """ Manage reading, writing, and tracking metadata entries for a file. """

    def __init__(self, file_path: Path | str):
        """ Initialize the MetadataManager """
        self.file_path = Path(file_path)
        self.meta_dir = self.file_path.parent / ".meta"
        self.meta_dir.mkdir(exist_ok=True)
        self.meta_file = self.meta_dir / f"{self.file_path.stem}.json"

    def read(self) -> list[MetadataEntry]:
        """ Read metadata from the JSON file. """
        if not self.meta_file.exists():
            return []
        with self.meta_file.open('r') as file:
            raw_meta = json.load(file)
        return [MetadataEntry.from_dict(entry) for entry in raw_meta]

    def read_last(self) -> MetadataEntry | None:
        """ Read the last metadata entry from the JSON file. """
        meta = self.read()
        if not meta:
            return None
        return meta[-1]

    def read_last_write(self) -> MetadataEntry | None:
        """ Read the last write metadata entry from the JSON file. """
        meta = self.read()
        for entry in reversed(meta):
            if entry.operation == "write":
                return entry
        return None

    def write(self, meta: MetadataEntry) -> None:
        """ Write metadata to the JSON file. """
        entries  = self.read()
        entries .append(meta)
        dict_entries = [e.to_dict() for e in entries]
        with self.meta_file.open('w') as file:
            json.dump(dict_entries, file, indent=4)
    
    def delete(self) -> None:
        """ Delete the metadata file. """
        try:
            self.meta_file.unlink()
        except FileNotFoundError:
            logger.warning(f"Metadata file {self.meta_file} does not exist, nothing to delete.")

    def record_write(
        self,
        added: int,
        removed: int = 0,
        source_versions: dict | None = None
    ) -> None:
        """ Record an append operation in the metadata. """
        last_meta = self.read_last()
        version = last_meta.version + 1 if last_meta else 1

        entry = MetadataEntry(
            timestamp=self.get_now(),
            operation="write",
            added=added,
            removed=removed,
            version=version,
            source_versions=source_versions
        )
        self.write(entry)

    def record_read(self) -> None:
        """ Record a read operation in the metadata. """
        last_meta = self.read_last()
        version = last_meta.version + 1 if last_meta else 1
        entry = MetadataEntry(
            timestamp=self.get_now(),
            operation="read",
            added=0,
            removed=0,
            version=version
        )
        self.write(entry)

    @staticmethod
    def get_now() -> str:
        """ Return the current timestamp in ISO format. """
        return datetime.now().isoformat(timespec="seconds")

# The metadata for each modification/reading operation:
# - Timestamp/datetime of the operation
# - Type of operation (read/write)
# - Added (optional, int)
# - Removed (optional, int)
# - Version
# - Source versions (dict[sourc_model: max_version & version_field])

# When a file is updated using a set of sources, here is what the updated metadata should contain:
# - Timestamp of the operation (self.now)
# - Type of operation (write)
# - Added (number of rows added)
# - Removed (number of rows removed)
# - Version (incremented version number)
# - Source versions ({'source_name': {'version_cutoff': version_cutoff, 'version_field': [stage]_at}})

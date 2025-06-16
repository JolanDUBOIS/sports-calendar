from pathlib import Path

from . import FileHandler, MetadataManager
from ..types import IOContent


class TrackedFileHandler:
    """ A file handler that tracks changes to the file and manages metadata. """

    def __init__(self, handler: FileHandler):
        """ Initialize the tracked file handler with a file handler. """
        self.handler = handler
        self.meta_manager = MetadataManager(handler.path)

    @property
    def path(self) -> Path:
        """ Return the file path of the underlying file handler. """
        return self.handler.path

    def __len__(self):
        """ Return the length of the content in the underlying file handler. """
        return len(self.handler)

    def __repr__(self):
        """ Return a string representation of the tracked file handler. """
        return f"{self.__class__.__name__}(file_path={self.path})"

    def read(self, *args, **kwargs) -> IOContent:
        """ Read the content of the file using the underlying file handler and update metadata. """
        data = self.handler.read(*args, **kwargs)
        self.meta_manager.record_read()
        return data

    def write(self, data: IOContent, source_versions: dict | None = None, *args, **kwargs):
        """ Write data to the file using the underlying file handler and update metadata. """
        added, removed = self.handler.write(data, *args, **kwargs)
        self.meta_manager.record_write(
            added=added,
            removed=removed,
            source_versions=source_versions or {}
        )

    def save(self) -> None:
        """ Save the changes to the file using the underlying file handler. """
        self.handler.save()

    def delete(self, *args, **kwargs):
        """ Delete data from the file using the underlying file handler and update metadata. """
        added, removed = self.handler.delete(*args, **kwargs)
        self.meta_manager.record_write(added=added, removed=removed)

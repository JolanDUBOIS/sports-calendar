from __future__ import annotations
from typing import TYPE_CHECKING

from . import logger
from .specs import OutputSpec
from .enforcers import UniqueEnforcer, NonNullableEnforcer
from ...versioning import read_versions
from ....utils import concat_io_content
from ....file_io import FileHandlerFactory

if TYPE_CHECKING:
    from .enforcers import OutputEnforcer
    from ...versioning import SourceVersions
    from ....types import IOContent


class OutputManager:
    """ TODO """

    def __init__(self, output_spec: OutputSpec):
        """ TODO """
        logger.debug(f"Initializing OutputManager with spec: {output_spec}")
        self.output_spec = output_spec
        self.handler = FileHandlerFactory.create_file_handler(self.output_spec.path, tracked=True)
        self.enforcers = self._init_enforcers()

    def _init_enforcers(self) -> list[OutputEnforcer]:
        """ Initialize enforcers based on the output specification. """
        enforcers = []
        if self.output_spec.unique:
            enforcers.append(UniqueEnforcer(self.output_spec.unique))
        if self.output_spec.non_nullable:
            enforcers.append(NonNullableEnforcer(self.output_spec.non_nullable))
        return enforcers

    def write(self, data: IOContent, source_versions: SourceVersions):
        """ TODO """
        try:
            existing_data = self.handler.read()
            full_data = concat_io_content(existing_data, data)
        except FileNotFoundError:
            logger.debug("No existing output file found. Proceeding with new data only.")
            full_data = data

        for enforcer in self.enforcers:
            logger.debug(f"Applying enforcer: {enforcer.__class__.__name__}")
            full_data = enforcer.apply(full_data)

        self.handler.write(full_data, source_versions=source_versions.to_dict(), overwrite=True)
        self.handler.save()

    def read_source_versions(self) -> SourceVersions:
        """ Read source versions from the output metadata. """
        metadata_entry = self.handler.meta_manager.read_last_write()
        return read_versions(metadata_entry)

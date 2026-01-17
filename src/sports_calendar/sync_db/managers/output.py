from __future__ import annotations
from typing import TYPE_CHECKING

from . import logger
from .enforcers import ConstraintEnforcerFactory
from ..versioning import read_versions
from sports_calendar.sync_db.utils import concat_io_content
from sports_calendar.core.file_io import FileHandlerFactory

if TYPE_CHECKING:
    from .enforcers import ConstraintEnforcer
    from ..versioning import SourceVersions
    from sports_calendar.core import IOContent
    from sports_calendar.sync_db.definitions.specs import OutputSpec


class OutputManager:
    """ Manage the output data writing process and enforce output constraints. """

    def __init__(self, output_spec: OutputSpec):
        """ Initialize with an output specification and prepare file handler and enforcers. """
        logger.debug(f"Initializing OutputManager with spec: {output_spec}")
        self.output_spec = output_spec
        self.handler = FileHandlerFactory.create_file_handler(self.output_spec.path)
        self.enforcers = self._init_enforcers()

    def _init_enforcers(self) -> list[ConstraintEnforcer]:
        """ Initialize enforcers based on the output specification. """
        enforcers = []
        for contraint_spec in self.output_spec.constraints:
            enforcers.append(ConstraintEnforcerFactory.create_enforcer(contraint_spec))
        return enforcers

    def write(self, data: IOContent, source_versions: SourceVersions):
        """ Write processed data to the output file, applying enforcers and saving metadata. """
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

    def reset(self):
        """ Reset the output file by deleting it. """
        logger.debug(f"Resetting output file: {self.output_spec.path}")
        self.handler.delete(force=True)

    def read_source_versions(self) -> SourceVersions:
        """ Retrieve the last recorded source versions from output metadata. """
        metadata_entry = self.handler.meta_manager.read_last_write()
        if metadata_entry:
            return read_versions(metadata_entry)
        else:
            return None

from __future__ import annotations
from typing import TYPE_CHECKING
from dataclasses import dataclass, field
from pathlib import Path

from . import logger
from .enforcers import ConstraintEnforcerFactory, ConstraintSpecs
from ..versioning import read_versions
from ...utils import concat_io_content
from ....file_io import FileHandlerFactory

if TYPE_CHECKING:
    from .enforcers import ConstraintEnforcer
    from ..versioning import SourceVersions
    from ....types import IOContent


@dataclass
class OutputSpec:
    name: str
    path: Path
    layer: str
    schema: str | None = None
    constraint_specs: ConstraintSpecs = field(default_factory=ConstraintSpecs)

    def __repr__(self):
        """ String representation of the OutputSpec. """
        return (
            f"OutputSpec(name={self.name}, path={self.path}, layer={self.layer}, "
            f"schema={self.schema}, constraint_specs={self.constraint_specs}"
        )

    @classmethod
    def from_dict(cls, d: dict) -> OutputSpec:
        """ Create an OutputSpec from a dictionary. """
        return cls(
            name=d["name"],
            path=Path(d["path"]),
            layer=d["layer"],
            schema=d.get("schema"),
            constraint_specs=ConstraintSpecs.from_dict(d)
        )

class OutputManager:
    """ Manage the output data writing process and enforce output constraints. """

    def __init__(self, output_spec: OutputSpec):
        """ Initialize with an output specification and prepare file handler and enforcers. """
        logger.debug(f"Initializing OutputManager with spec: {output_spec}")
        self.output_spec = output_spec
        self.handler = FileHandlerFactory.create_file_handler(self.output_spec.path, tracked=True)
        self.enforcers = self._init_enforcers()

    def _init_enforcers(self) -> list[ConstraintEnforcer]:
        """ Initialize enforcers based on the output specification. """
        enforcers = []
        for contraint_spec in self.output_spec.constraint_specs:
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
        self.handler.save()

    def reset(self):
        """ Reset the output file by deleting it. """
        logger.debug(f"Resetting output file: {self.output_spec.path}")
        self.handler.delete(force=True)
        self.handler.save()

    def read_source_versions(self) -> SourceVersions:
        """ Retrieve the last recorded source versions from output metadata. """
        metadata_entry = self.handler.meta_manager.read_last_write()
        if metadata_entry:
            return read_versions(metadata_entry)
        else:
            return None

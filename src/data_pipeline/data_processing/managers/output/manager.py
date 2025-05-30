from __future__ import annotations
from typing import TYPE_CHECKING

from .specs import OutputSpec
from .enforcers import UniqueEnforcer, NonNullableEnforcer
from ...versioning import read_versions
from ....file_io import FileHandlerFactory

if TYPE_CHECKING:
    from .enforcers import OutputEnforcer
    from ...versioning import SourceVersions
    from ....types import IOContent


class OutputManager:
    """ TODO """

    def __init__(self, output_spec: OutputSpec):
        """ TODO """
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
        """ Write data to the output file. """
        for enforcer in self.enforcers:
            data = enforcer.apply(data)

        self.handler.write(data, source_versions=source_versions.to_dict())
        self.handler.save()

    def read_source_versions(self) -> SourceVersions:
        """ Read source versions from the output metadata. """
        metadata_entries = self.handler.meta_manager.read()
        return read_versions(metadata_entries)

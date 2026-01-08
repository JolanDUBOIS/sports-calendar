from __future__ import annotations
from typing import TYPE_CHECKING

from ..processors import ProcessorFactory
from sports_calendar.sc_core import IOContent

if TYPE_CHECKING:
    from sports_calendar.sync_db.definitions.specs import ProcessingStepSpec


class ProcessingManager:
    """ Manage data processing using a processing specification. """

    def __init__(self, processing_spec: list[ProcessingStepSpec]):
        """ Initialize with a processing specification. """
        self.processing_spec = processing_spec

    def process(self, sources: dict[str, IOContent]) -> dict[str, IOContent]:
        """ Process the data according to the processing specification. """
        data = sources.copy()
        for step in self.processing_spec:
            processor = ProcessorFactory.create_processor(step.processor)
            data = processor.run(data=data, processing_step_spec=step)
        return data

from __future__ import annotations
from typing import TYPE_CHECKING, Any
from dataclasses import dataclass

from . import logger
from ..processors import ProcessorFactory

if TYPE_CHECKING:
    from ....types import IOContent


@dataclass
class ProcessingSpec:
    """ Describes how to process the data (e.g., transformations, filters). """
    processor_name: str
    parameters: dict[str, Any]

    @classmethod
    def from_dict(cls, d: dict) -> ProcessingSpec:
        """ Create a ProcessingSpec from a dictionary. """
        return cls(
            processor_name=d["processor"],
            parameters={k: v for k, v in d.items() if k != "processor"}
        )

class ProcessingManager:
    """ Manage data processing using a specified processing specification. """

    def __init__(self, processing_spec: ProcessingSpec):
        """ Initialize with a processing specification. """
        self.processing_spec = processing_spec

    def process(self, sources: dict[str, IOContent]) -> IOContent:
        """ Process the data according to the processing specification. """
        processor = ProcessorFactory.get_processor(self.processing_spec.processor_name)
        logger.debug(f"Using processor: {processor.__class__.__name__} with parameters: {self.processing_spec.parameters}")
        return processor.run(
            sources=sources,
            **self.processing_spec.parameters
        )

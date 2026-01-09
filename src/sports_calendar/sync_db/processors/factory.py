from typing import Type

from . import logger
from .base import Processor
from .client import ClientProcessor
from .date_standardization import DateStandardizationProcessor
from .reshaping import ReshapingProcessor
from .parsing import ParsingProcessor
from .json_extraction import JsonExtractionProcessor
from .table_extraction import TableExtractionProcessor
from .remapping import RemappingProcessor


PROCESSORS_REGISTRY = {
    "ClientProcessor": ClientProcessor,
    "DateStandardizationProcessor": DateStandardizationProcessor,
    "JsonExtractionProcessor": JsonExtractionProcessor,
    "ParsingProcessor": ParsingProcessor,
    "RemappingProcessor": RemappingProcessor,
    "ReshapingProcessor": ReshapingProcessor,
    "TableExtractionProcessor": TableExtractionProcessor,
}

class ProcessorFactory:
    """ Factory class to create processor instances based on the processor name. """

    @staticmethod
    def create_processor(processor_name: str) -> Type[Processor]:
        """ Create a processor instance based on the processor name. """
        if processor_name not in PROCESSORS_REGISTRY:
            logger.error(f"Processor '{processor_name}' is not registered.")
            raise ValueError(f"Processor '{processor_name}' is not registered.")
        
        processor_class = PROCESSORS_REGISTRY[processor_name]
        return processor_class

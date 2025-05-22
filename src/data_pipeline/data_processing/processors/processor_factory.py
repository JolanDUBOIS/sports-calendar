from .processor_base_class import Processor
from .derivation import DerivationProcessor
from .extraction import ExtractionProcessor
from .client import ClientProcessor
from .parsing import ParsingProcessor
from .registry import RegistryProcessor


class ProcessorFactory:
    """ TODO """

    processors = {
        "DerivationProcessor": DerivationProcessor,
        "ExtractionProcessor": ExtractionProcessor,
        "ClientProcessor": ClientProcessor,
        "ParsingProcessor": ParsingProcessor,
        "RegistryProcessor": RegistryProcessor,
    }

    @classmethod
    def get_processor(cls, name: str) -> Processor:
        """ TODO """
        return cls.processors.get(name)()

from .processor_base_class import Processor
from .landing.client import ClientProcessor
from .intermediate import DerivationProcessor, ExtractionProcessor, ParsingProcessor, RegistryProcessor


class ProcessorFactory:
    """ Factory class to create processor instances by name. """

    processors = {
        "DerivationProcessor": DerivationProcessor,
        "ExtractionProcessor": ExtractionProcessor,
        "ClientProcessor": ClientProcessor,
        "ParsingProcessor": ParsingProcessor,
        "RegistryProcessor": RegistryProcessor,
    }

    @classmethod
    def get_processor(cls, name: str) -> Processor:
        """ Return a processor instance based on the given name. """
        return cls.processors.get(name)()

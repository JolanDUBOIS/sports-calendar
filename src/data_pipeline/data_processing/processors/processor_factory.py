from . import logger
from .processor_base_class import Processor
from .landing import *
from .intermediate import *


class ProcessorFactory:
    """ Factory class to create processor instances by name. """

    @classmethod
    def _all_processors(cls) -> dict[str, type[Processor]]:
        """Discover all subclasses of Processor and return them as a name-to-class map."""
        def all_subclasses(cls_):
            return set(cls_.__subclasses__()).union(
                s for c in cls_.__subclasses__() for s in all_subclasses(c)
            )

        return {
            subclass.__name__: subclass
            for subclass in all_subclasses(Processor)
        }

    @classmethod
    def get_processor(cls, name: str) -> Processor:
        """Return a processor instance by class name."""
        processors = cls._all_processors()
        logger.debug(f"Available processors: {list(processors.keys())}")
        if name not in processors:
            logger.error(f"Processor '{name}' not found in factory.")
            raise ValueError(f"Processor '{name}' not found in factory.")
        return processors[name]()

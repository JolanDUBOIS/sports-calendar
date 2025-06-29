from . import logger
from .base import EventTransformer
from .f1 import F1EventTransformer
from .football import FootballEventTransformer


class EventTransformerFactory:
    """ Factory class to create event transformers based on sport type. """

    mapping = {
        'football': FootballEventTransformer,
        'f1': F1EventTransformer,
    }

    @classmethod
    def create_transformer(cls, sport: str) -> EventTransformer:
        """ Get the appropriate event transformer for the given sport. """
        transformer = cls.mapping.get(sport.lower())
        if not transformer:
            logger.error(f"No transformer found for sport: {sport}")
            raise ValueError(f"No transformer found for sport: {sport}")
        return transformer()

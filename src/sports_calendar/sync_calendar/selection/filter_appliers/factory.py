from . import logger
from .base import FilterApplier
from .f1 import F1FilterApplier
from .football import FootballFilterApplier


class FilterApplierFactory:
    """ Factory class to create filter appliers based on entity type. """

    applier_map = {
        "f1": F1FilterApplier,
        "football": FootballFilterApplier,
    }

    @staticmethod
    def create_applier(sport: str) -> FilterApplier:
        """ Create a filter applier based on the entity type. """
        if sport not in FilterApplierFactory.applier_map:
            logger.error(f"Unsupported sport type: {sport}")
            raise ValueError(f"Unsupported sport type: {sport}")
        applier_class = FilterApplierFactory.applier_map[sport]
        return applier_class()

from . import logger
from .base import SelectionResolver
from .f1 import F1SelectionResolver
from .football import FootballSelectionResolver
from ..filter_appliers import FilterApplierFactory


class SelectionResolverFactory:
    """ Factory class to create selection resolvers based on entity type. """

    resolver_map = {
        "f1": F1SelectionResolver,
        "football": FootballSelectionResolver,
    }

    @staticmethod
    def create_resolver(sport: str) -> SelectionResolver:
        """ Create a resolver based on the entity type. """
        if sport not in SelectionResolverFactory.resolver_map:
            logger.error(f"Unsupported sport type: {sport}")
            raise ValueError(f"Unsupported sport type: {sport}")
        resolver_class = SelectionResolverFactory.resolver_map[sport]
        filter_applier = FilterApplierFactory.create_applier(sport)
        return resolver_class(filter_applier)

from sportindex import SportClient, EventCollection

from . import logger
from .executors import EXECUTOR_MAP
from sports_calendar.core import SportType
from sports_calendar.core.utils import validate
from sports_calendar.core.selection import Selection, SelectionItem


class Resolver:
    """ TODO """

    @classmethod
    def resolve_selection(cls, selection: Selection, client: SportClient, **kwargs) -> list[tuple[EventCollection, SportType]]:
        """ Resolve a Selection into a list of tuples containing an EventCollection and its corresponding SportType. """
        logger.debug(f"Resolving selection: {selection.name} with {len(selection.items)} items...")
        validate(isinstance(selection, Selection), "Selection must be an instance of Selection for resolution.", logger)
        validate(isinstance(client, SportClient), "Client must be an instance of SportClient for resolution.", logger)

        resolved_collections: list[tuple[EventCollection, SportType]] = []

        for item in selection.items:
            events = cls._resolve_item(item, client, **kwargs)
            resolved_collections.append((events, item.sport))

        logger.debug(f"Selection {selection.name} resolved with {len(resolved_collections)} collections.")
        return resolved_collections

    @classmethod
    def _resolve_item(cls, item: SelectionItem, client: SportClient, **kwargs) -> EventCollection:
        """ Resolve a SelectionFilter into a collection of events. """
        logger.debug(f"Resolving filter: {item.name}...")
        validate(isinstance(item, SelectionItem), "Item must be an instance of SelectionItem for resolution.", logger)
        validate(isinstance(client, SportClient), "Client must be an instance of SportClient for resolution.", logger)

        events = None
        for filter in item.filters:
            executor = EXECUTOR_MAP.get(filter.filter_type)

            if not executor:
                raise ValueError(f"No executor found for filter type {filter.filter_type} in football resolver.")

            logger.debug(f"Executing filter {filter.name} of type {filter.filter_type} using {executor.__name__}...")
            if events is None:
                events = executor.fetch(filter.fields, client)
            else:
                events = executor.apply(filter.fields, events)
            logger.debug(f"Filter {filter.name} applied, resulting in {len(events)} events.")

        logger.debug(f"Finished resolving item: {item.name} with {len(events) if events else 0} events.")
        return events or EventCollection()

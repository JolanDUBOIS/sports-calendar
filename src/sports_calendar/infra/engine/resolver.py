import logging

from sportindex import EventCollection, SportClient

from sports_calendar.core.selection import Selection, SelectionItem
from sports_calendar.core.utils import validate

from .executors import EXECUTOR_MAP

logger = logging.getLogger(__name__)


class Resolver:
    """ TODO """

    @classmethod
    def resolve_selection(cls, selection: Selection, client: SportClient, **kwargs) -> list[tuple[EventCollection, int]]:
        """ Resolve a Selection into a list of tuples containing an EventCollection and its corresponding sport id. """
        logger.info(f"Resolving selection: {selection.name} with {len(selection.items)} items...")
        validate(isinstance(selection, Selection), "Selection must be an instance of Selection for resolution.", logger)
        validate(isinstance(client, SportClient), "Client must be an instance of SportClient for resolution.", logger)

        resolved_collections: list[tuple[EventCollection, int]] = []

        for item in selection.items:
            events = cls._resolve_item(item, client, **kwargs)
            resolved_collections.append((events, item.sport_id))

        logger.info(f"Selection {selection.name} resolved with {len(resolved_collections)} collections.")
        return resolved_collections

    @classmethod
    def _resolve_item(cls, item: SelectionItem, client: SportClient, **kwargs) -> EventCollection:
        """ Resolve a SelectionItem into a collection of events. """
        logger.info(f"Resolving filter: {item.name}...")
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
                events = executor.apply(filter.fields, events, client)
            logger.debug(f"Filter {filter.name} applied, resulting in {len(events)} events.")

        logger.info(f"Finished resolving item: {item.name} with {len(events) if events else 0} events.")
        return events or EventCollection()

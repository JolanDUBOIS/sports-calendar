import logging

from sportindex import EventCollection, SportClient

from sports_calendar.core.selection import Selection, SelectionItem
from sports_calendar.core.utils import validate

from .executors import EXECUTOR_MAP

logger = logging.getLogger(__name__)


class Resolver:
    """ Turns a saved Selection into the events it stands for.

    Everything unions. A selection is the union of its items, an item is the
    union of its filters, and each filter is a self-contained rule that already
    carries its own narrowing — `from_round` on a competitions filter, a
    selection rule on a competitors one, a cut-off on a ranking one.

    This used to intersect an item's filters, chaining them so that the first
    fetched and the rest narrowed. That read naturally from the word "filter",
    but it is not what the app promises or what anyone builds: rules get named
    "All PSG games" and "Supercups" — a list of things to follow, not a set of
    conditions to satisfy at once. Intersecting them produced an empty calendar
    the moment two of them named different competitions, which is to say almost
    immediately.

    Unioning also makes filter order irrelevant, where before it decided which
    filter fetched and which merely narrowed — and `fetch` and `apply` are not
    the same predicate for every executor.
    """

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
        """ Everything an item's filters ask for, merged.

        Each filter is fetched independently, so no filter can suppress another
        and the order they are stored in has no effect. `EventCollection.__or__`
        deduplicates, so a fixture named by two filters is carried once.

        A filter that resolves to nothing — an empty one, or a competition that
        has gone stale — contributes nothing rather than emptying the item.
        """
        logger.info(f"Resolving item: {item.name}...")
        validate(isinstance(item, SelectionItem), "Item must be an instance of SelectionItem for resolution.", logger)
        validate(isinstance(client, SportClient), "Client must be an instance of SportClient for resolution.", logger)

        events = EventCollection()
        for filter in item.filters:
            executor = EXECUTOR_MAP.get(filter.filter_type)

            if not executor:
                raise ValueError(f"No executor found for filter type {filter.filter_type}.")

            logger.debug(f"Executing filter {filter.name} of type {filter.filter_type} using {executor.__name__}...")
            matched = executor.fetch(filter.fields, client)
            logger.debug(f"Filter {filter.name} matched {len(matched)} events.")
            events |= matched

        logger.info(f"Finished resolving item: {item.name} with {len(events)} events.")
        return events

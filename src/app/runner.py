from .selection import Selection
from .selection.selection_resolvers import SelectionResolverFactory
from .calendar.sports_calendar import SportsEventCollection
from .calendar.sports_calendar.event_transformers import EventTransformerFactory


class SelectionRunner:
    """ TODO """

    def __init__(self, selection: Selection):
        self.selection = selection

    def run(self, date_from: str | None = None, date_to: str | None = None) -> SportsEventCollection:
        """ TODO """
        events = SportsEventCollection()
        for item in self.selection:
            resolver = SelectionResolverFactory.create_resolver(item.sport)
            item_events = resolver.get_events(item, date_from, date_to)
            transformer = EventTransformerFactory.create_transformer(item.sport)
            events.extend(transformer.batch_transform(item_events))
        # Deduplicate events
        events.drop_duplicates(inplace=True)
        return events

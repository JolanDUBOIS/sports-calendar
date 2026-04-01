from sportindex import SportClient, EventCollection

from .base import BaseExecutor
from sports_calendar.core.selection import EmptyFilterFields


class EmptyExecutor(BaseExecutor[EmptyFilterFields]):
    """ Executor for empty filters. """

    @classmethod
    def fetch(cls, filter_fields: EmptyFilterFields, client: SportClient) -> EventCollection:
        """ For empty filters, we return an empty EventCollection. """
        return EventCollection()

    @classmethod
    def apply(cls, filter_fields: EmptyFilterFields, events: EventCollection, client: SportClient) -> EventCollection:
        """ For empty filters, we return the input events unchanged. """
        return events

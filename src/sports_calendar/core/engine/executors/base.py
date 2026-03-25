from abc import ABC, abstractmethod
from typing import TypeVar, Generic

from sportindex import SportClient, EventCollection

from sports_calendar.core.selection import FilterFields


T = TypeVar('T', bound=FilterFields)

class BaseExecutor(ABC, Generic[T]):
    """ Base class for all executors. """

    @classmethod
    @abstractmethod
    def fetch(cls, filter_fields: T, client: SportClient) -> EventCollection:
        """ Fetch events using the given filter fields and the provided SportClient. """
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def apply(cls, filter_fields: T, events: EventCollection) -> EventCollection:
        """ Apply the given filter fields to the provided events. """
        raise NotImplementedError

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

from icalendar import Event as ICalendarEvent

if TYPE_CHECKING:
    from datetime import datetime

    import sportindex


class SportsEvent(ABC):
    """ Abstract base class for sports events. """

    def __init__(self, source_id: str | None = None, **kwargs):
        """ Initialize the sports event with common attributes.

        `source_id` is the provider's own id for this event (`mch:12345`). It is
        what lets a synced calendar recognise an event it has already written,
        so a fixture that moves is updated in place rather than removed and
        recreated. Deliberately not derived from the event's details: a Ligue 1
        fixture is pencilled in for a Sunday and only given its real kick-off a
        month out, and it is the same match throughout.
        """
        self.source_id = source_id
        self.extra = kwargs

    def __repr__(self):
        """ Return a string representation of the event. """
        return f"{self.__class__.__name__}(summary={self.summary}, start={self.start}, end={self.end})"

    @property
    @abstractmethod
    def summary(self) -> str:
        raise NotImplementedError("Subclasses must implement summary property for event details.")

    @property
    @abstractmethod
    def start(self) -> datetime:
        raise NotImplementedError("Subclasses must implement start property for event details.")

    @property
    @abstractmethod
    def end(self) -> datetime:
        raise NotImplementedError("Subclasses must implement end property for event details.")

    @property
    @abstractmethod
    def location(self) -> str:
        raise NotImplementedError("Subclasses must implement location property for event details.")

    @property
    @abstractmethod
    def description(self) -> str:
        raise NotImplementedError("Subclasses must implement description property for event details.")

    def get_event(self) -> ICalendarEvent:
        """ Convert this sports event into an iCalendar event.

        Optional fields are omitted when empty rather than written blank:
        icalendar stringifies whatever it is given, so a missing venue used to
        reach the calendar as the literal text "None".
        """
        event = ICalendarEvent()
        # The provider's id, carried so a sync can match this against an event
        # it wrote before. UID is the standard iCalendar field for it; how a
        # given calendar service stores it is that service's problem.
        if self.source_id:
            event.add("uid", self.source_id)
        event.add("summary", self.summary)
        event.add("dtstart", self.start)
        event.add("dtend", self.end)
        if self.location:
            event.add("location", self.location)
        if self.description:
            event.add("description", self.description)
        return event

    @abstractmethod
    def identity_key(self) -> str:
        """ Return a unique string identifying this event for equality/deduplication. """
        raise NotImplementedError("Subclasses must implement identity_key method for event deduplication.")

    @classmethod
    @abstractmethod
    def from_sport_index_event(cls, event: sportindex.Event) -> SportsEvent:
        """ Factory method to create a SportsEvent from a sportindex.Event. """
        raise NotImplementedError("Subclasses must implement from_sport_index_event class method for event creation from SportIndex data.")


class SportsEventCollection:
    """ Collection of sports events. """

    def __init__(self, events: list[SportsEvent] | None = None):
        self.events = events or []

    def __iter__(self):
        """ Iterate over the events in the collection. """
        return iter(self.events)

    def __len__(self) -> int:
        """ Return the number of events in the collection. """
        return len(self.events)

    def __getitem__(self, index: int | slice) -> SportsEvent | list[SportsEvent]:
        return self.events[index]

    def __add__(self, other: SportsEventCollection) -> SportsEventCollection:
        """ Combine two collections into a new collection. """
        if not isinstance(other, SportsEventCollection):
            return NotImplemented
        return SportsEventCollection(self.events + other.events)

    def __iadd__(self, other: SportsEventCollection) -> SportsEventCollection:
        """ Extend this collection with another collection. """
        if not isinstance(other, SportsEventCollection):
            return NotImplemented
        self.events.extend(other.events)
        return self

    def __repr__(self):
        """ Return a string representation of the collection with one event per line. """
        if not self.events:
            return f"{self.__class__.__name__}(events=[])"
        indented_events = "\n  ".join(repr(event) for event in self.events)
        return f"{self.__class__.__name__}([\n  {indented_events}\n])"

    def append(self, event: SportsEvent) -> None:
        """ Append a single event to the collection. """
        self.events.append(event)

    def extend(self, events: list[SportsEvent]) -> None:
        """ Extend the collection with a list of events. """
        self.events.extend(events)

    def drop_duplicates(self, inplace: bool = False) -> SportsEventCollection:
        """ Remove duplicate events based on their identity key. """
        seen = set()
        unique = []
        for event in self.events:
            key = event.identity_key()
            if key not in seen:
                seen.add(key)
                unique.append(event)
        if inplace:
            self.events = unique
            return self
        return SportsEventCollection(unique)

    @classmethod
    def from_sport_index_collection(cls, collection: sportindex.EventCollection, event_cls: type[SportsEvent]) -> SportsEventCollection:
        """ Factory method to create a SportsEventCollection from a sportindex.EventCollection. """
        events = [event_cls.from_sport_index_event(event) for event in collection]
        return cls(events)

    # TODO: Instead of drop_duplicates, implement a & and | operator for intersection and union of collections...

from __future__ import annotations
from abc import ABC, abstractmethod

from icalendar import Event

from datetime import datetime


class SportsEvent(ABC):
    """ Abstract base class for sports events. """
    sport: str

    def __repr__(self):
        """ Return a string representation of the event. """
        return f"{self.__class__.__name__}(sport={self.sport}, summary={self.summary}, start={self.start}, end={self.end})"

    @property
    @abstractmethod
    def summary(self) -> str:
        """ TODO """

    @property
    @abstractmethod
    def start(self) -> datetime:
        """ TODO """

    @property
    @abstractmethod
    def end(self) -> datetime:
        """ TODO """

    @property
    @abstractmethod
    def location(self) -> str:
        """ TODO """

    @property
    @abstractmethod
    def description(self) -> str:
        """ TODO """

    def get_event(self) -> Event:
        event = Event()
        event.add("summary", self.summary)
        event.add("dtstart", self.start)
        event.add("dtend", self.end)
        event.add("location", self.location)
        event.add("description", self.description)
        return event

    @abstractmethod
    def identity_key(self) -> str:
        """ Return a unique string identifying this event for equality/deduplication. """
        pass


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
        else:
            return SportsEventCollection(unique)

from __future__ import annotations
from datetime import datetime, timedelta

from sportindex import Event as SportIndexEvent

from . import logger
from .base import SportsEvent
from sports_calendar.core import SportType


class F1Event(SportsEvent):
    """ Represents a Formula 1 event. """
    sport = SportType.F1

    def __init__(
        self,
        start: datetime | str,
        name: str,
        session: str,
        city: str = None,
        country: str = None,
        **kwargs
    ):
        """ Initialize the F1Event with event details. """
        self._start = start
        self.name = name
        self.session = session
        self.city = city
        self.country = country
        super().__init__(**kwargs)

    @property
    def summary(self) -> str:
        """ Summary of the F1 event. """
        return f"{self.name} ({self.session})"

    @property
    def start(self) -> datetime:
        """ Start time of the F1 event. """
        return self._start if isinstance(self._start, datetime) else datetime.fromisoformat(self._start)

    @property
    def end(self) -> datetime:
        """ End time of the F1 event. """
        return self.start + timedelta(hours=2)

    @property
    def location(self) -> str:
        """ Location of the F1 event. """
        return f"{self.city}, {self.country}" if self.city and self.country else ""

    @property
    def description(self) -> str:
        """ Description of the F1 event. """
        description_lines = [f"Sport: {self.sport}"]
        description_lines.append(f"Session: {self.session}")
        if self.city:
            description_lines.append(f"City: {self.city}")
        if self.country:
            description_lines.append(f"Country: {self.country}")
        return "\n".join(description_lines)

    def identity_key(self) -> str:
        """ Return a unique string identifying this event for equality/deduplication. """
        return f"{self.sport} | {self.name} | {self.session} | {self.start}"

    @classmethod
    def from_sport_index_event(cls, event: SportIndexEvent) -> F1Event:
        """ Factory method to create an F1Event from a SportIndexEvent. """
        return cls(
            start=event.start,
            name=event.source.parent.description, # NOTE - Expecting an update of the SportIndexEvent model to include a 'parent' field in race events
            session=event.name,
            city=event.venue.city if event.venue else None,
            country=event.venue.country.name if event.venue and event.venue.country else None,
            sport_idx_event=event
        )

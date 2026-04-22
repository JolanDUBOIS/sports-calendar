from __future__ import annotations
from datetime import datetime, timedelta

import sportindex

from .base import SportsEvent


class StageEvent(SportsEvent):
    """ Represents a stage event (comparison sport event). """

    def __init__(
        self,
        start: datetime | str,
        sport: str = "unknown",
        name: str = "",
        session: str = "",
        city: str = None,
        country: str = None,
        **kwargs
    ):
        """ Initialize the StageEvent with event details. """
        self._start = start
        self.sport = sport

        self.name = name
        self.session = session

        self.city = city
        self.country = country

        super().__init__(**kwargs)

    @property
    def summary(self) -> str:
        """ Summary of the Stage event. """
        return f"{self.name} ({self.session})"

    @property
    def start(self) -> datetime:
        """ Start time of the Stage event. """
        return self._start if isinstance(self._start, datetime) else datetime.fromisoformat(self._start)

    @property
    def end(self) -> datetime:
        """ End time of the Stage event. """
        return self.start + timedelta(hours=2)

    @property
    def location(self) -> str:
        """ Location of the Stage event. """
        return f"{self.city}, {self.country}" if self.city and self.country else ""

    @property
    def description(self) -> str:
        """ Description of the Stage event. """
        description_lines = [f"Sport: {self.sport}"] # TODO - Fetch sport name from id ? In the case of motorsport, this includes fetching the category (F1, MotoGP, etc.)
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
    def from_sport_index_event(cls, event: sportindex.StageEvent) -> StageEvent:
        """ Factory method to create a StageEvent from a sportindex.StageEvent. """
        return cls(
            start=event.start,
            sport=event.sport.name,
            name=event.parent.name, # NOTE - Check if this name is correct
            session=event.name,
            city=event.venue.city if event.venue else None,
            country=event.venue.country.name if event.venue and event.venue.country else None,
            sport_idx_event=event
        )

from __future__ import annotations
from datetime import datetime, timedelta

from . import logger
from .base import SportsEvent


class F1Event(SportsEvent):
    """ Represents a Formula 1 event. """
    sport = "f1"

    def __init__(
        self,
        name: str,
        session: str,
        date_time: str,
        city: str = None,
        country: str = None,
        **kwargs
    ):
        """ Initialize the F1Event with event details. """
        self.name = name
        self.session = session
        self.date_time = date_time
        self.city = city
        self.country = country

    @property
    def summary(self) -> str:
        """ Summary of the F1 event. """
        return f"{self.name} ({self.session})"

    @property
    def start(self) -> datetime:
        """ Start time of the F1 event. """
        # logger.debug(f"Parsing date_time: {self.date_time}")
        return datetime.fromisoformat(self.date_time)

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
        return f"{self.sport} | {self.name} | {self.session} | {self.date_time}"

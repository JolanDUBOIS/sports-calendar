from __future__ import annotations
from datetime import datetime, timedelta

from sportindex import Event as SportIndexEvent

from . import logger
from .base import SportsEvent
from sports_calendar.core import SportType


class FootballEvent(SportsEvent):
    """ Represents a football match event. """
    sport = SportType.FOOTBALL

    def __init__(
        self,
        start: datetime | str,
        home_team_id: str = None,
        home_team_name: str = "",
        home_team_abbreviation: str = None,
        away_team_id: str = None,
        away_team_name: str = "",
        away_team_abbreviation: str = None,
        competition_id: str = None,
        competition_name: str = None,
        stage: str = None,
        leg: str = None,
        venue: str = None,
        **kwargs
    ):
        """ Initialize the FootballEvent with match details. """
        self._start = start

        self.home_team_id = home_team_id
        self.home_team_name = home_team_name
        self.home_team_abbreviation = home_team_abbreviation

        self.away_team_id = away_team_id
        self.away_team_name = away_team_name
        self.away_team_abbreviation = away_team_abbreviation

        self.competition_id = competition_id
        self.competition_name = competition_name

        self.stage = stage
        self.leg = leg

        self._venue = venue

        super().__init__(**kwargs)

    @property
    def summary(self) -> str:
        """ TODO """
        return f"{self.home_team_name} - {self.away_team_name} ({self.competition_name})"

    @property
    def start(self) -> datetime:
        """ TODO """
        return self._start if isinstance(self._start, datetime) else datetime.fromisoformat(self._start) 

    @property
    def end(self) -> datetime:
        """ TODO """
        return self.start + timedelta(hours=2)

    @property
    def location(self) -> str:
        """ TODO """
        return self._venue

    @property
    def description(self) -> str:
        """ TODO """
        description_lines = [f"Sport: {self.sport}"]
        if self.competition_name:
            description_lines.append(f"Competition: {self.competition_name}")
        if self.stage:
            description_lines.append(f"Stage: {self.stage}")
        if self.leg:
            description_lines.append(f"Leg: {self.leg}")
        return "\n".join(description_lines) if description_lines else None

    def identity_key(self) -> str:
        """ Return a unique string identifying this event for equality/deduplication. """
        return f"{self.sport} | {self.home_team_name} vs {self.away_team_name} | {self.start}"

    @classmethod
    def from_sport_index_event(cls, event: SportIndexEvent) -> FootballEvent:
        """ Factory method to create a FootballEvent from a SportIndexEvent. """
        return cls(
            start=event.start,
            home_team_id=event.competitors.home.id if event.competitors and event.competitors.home else None,
            home_team_name=event.competitors.home.name if event.competitors and event.competitors.home else "",
            home_team_abbreviation=event.competitors.home.name_code if event.competitors and event.competitors.home else None,
            away_team_id=event.competitors.away.id if event.competitors and event.competitors.away else None,
            away_team_name=event.competitors.away.name if event.competitors and event.competitors.away else "",
            away_team_abbreviation=event.competitors.away.name_code if event.competitors and event.competitors.away else None,
            competition_id=event.competition.id if event.competition else None,
            competition_name=event.competition.name if event.competition else None,
            stage=event.round.name if event.round else None,
            leg=event.round.round if event.round else None,
            venue=event.venue.name if event.venue else None,
            sport_idx_event=event
        )

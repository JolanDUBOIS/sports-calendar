from __future__ import annotations
from datetime import datetime, timedelta

import pandas as pd

from .base import SportsEvent


class FootballEvent(SportsEvent):
    """ Represents a football match event. """
    sport = "football"

    def __init__(
        self,
        home_team_name: str,
        away_team_name: str,
        date_time: str,
        competition_name: str = None,
        competition_abbreviation: str = None,
        stage: str = None,
        leg: str = None,
        venue: str = None,
        **kwargs
    ):
        """ Initialize the FootballEvent with match details. """
        self.home_team_name = home_team_name
        self.away_team_name = away_team_name
        self.date_time = date_time
        self.competition_name = competition_name
        self.competition_abbreviation = competition_abbreviation
        self.stage = stage
        self.leg = leg

        self._venue = venue

    @property
    def summary(self) -> str:
        """ TODO """
        return f"{self.home_team_name} - {self.away_team_name} ({self.competition_abbreviation})"

    @property
    def start(self) -> datetime:
        """ TODO """
        return datetime.fromisoformat(self.date_time)

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
        return f"{self.sport} | {self.home_team_name} vs {self.away_team_name} | {self.date_time}"

from __future__ import annotations

from datetime import datetime, timedelta
from typing import TYPE_CHECKING

from .base import SportsEvent

if TYPE_CHECKING:
    import sportindex


class MatchEvent(SportsEvent):
    """ Represents an match event (opposition sport event). """

    def __init__(
        self,
        start: datetime | str,
        sport: str = "unknown",
        home_competitor_id: str = None,
        home_competitor_name: str = "",
        away_competitor_id: str = None,
        away_competitor_name: str = "",
        competition_id: str = None,
        competition_name: str = None,
        stage: str = None,
        leg: str = None,
        venue: str = None,
        **kwargs
    ):
        """ Initialize the MatchEvent with match details. """
        self._start = start
        self.sport = sport

        self.home_competitor_id = home_competitor_id
        self.home_competitor_name = home_competitor_name

        self.away_competitor_id = away_competitor_id
        self.away_competitor_name = away_competitor_name

        self.competition_id = competition_id
        self.competition_name = competition_name

        self.stage = stage
        self.leg = leg

        self._venue = venue

        super().__init__(**kwargs)

    @property
    def summary(self) -> str:
        """ Summary of the Match event. """
        return f"{self.home_competitor_name} - {self.away_competitor_name} ({self.competition_name})"

    @property
    def start(self) -> datetime:
        """ Start time of the Match event. """
        return self._start if isinstance(self._start, datetime) else datetime.fromisoformat(self._start)

    @property
    def end(self) -> datetime:
        """ End time of the Match event. """
        return self.start + timedelta(hours=2)

    @property
    def location(self) -> str:
        """ Location of the Match event.

        Empty rather than None when the provider names no venue — common for
        rugby — so callers can treat it as the string this signature promises.
        """
        return self._venue or ""

    @property
    def description(self) -> str:
        """ Description of the Match event. """
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
        return f"{self.sport} | {self.home_competitor_name} vs {self.away_competitor_name} | {self.start}"

    @classmethod
    def from_sport_index_event(cls, event: sportindex.MatchEvent) -> MatchEvent:
        """ Factory method to create a MatchEvent from a sportindex.MatchEvent. """
        return cls(
            start=event.start,
            sport=event.sport.name,
            home_competitor_id=event.competitors.home.id if event.competitors and event.competitors.home else None,
            home_competitor_name=event.competitors.home.name if event.competitors and event.competitors.home else "",
            away_competitor_id=event.competitors.away.id if event.competitors and event.competitors.away else None,
            away_competitor_name=event.competitors.away.name if event.competitors and event.competitors.away else "",
            competition_id=event.competition.id if event.competition else None,
            competition_name=event.competition.name if event.competition else None,
            stage=event.round.name if event.round else None,
            # `Round.value` is a matchday number in a league (Ligue 1 rounds
            # 1..34) but an opaque provider id in a knockout competition (the
            # Champions League playoff round is 636). Only the league case is a
            # meaningful "leg", and a named round is what marks the other, so
            # the number is dropped whenever the round carries a name.
            leg=event.round.value if event.round and not event.round.name else None,
            venue=event.venue.name if event.venue else None,
            source_id=str(event.id),
            sport_idx_event=event
        )

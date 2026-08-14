from __future__ import annotations

from datetime import datetime, timedelta
from typing import TYPE_CHECKING

from .base import SportsEvent
from .tiers import stage_tier_label

if TYPE_CHECKING:
    import sportindex


class StageEvent(SportsEvent):
    """ Represents a stage event (comparison sport event). """

    def __init__(
        self,
        start: datetime | str,
        sport: str = "unknown",
        name: str = "",
        session: str = "",
        session_name: str = None,
        competition_name: str = None,
        city: str = None,
        country: str = None,
        end: datetime | str | None = None,
        **kwargs
    ):
        """ Initialize the StageEvent with event details. """
        self._start = start
        self._end = end
        self.sport = sport

        self.name = name
        self.session = session
        self.session_name = session_name
        self.competition_name = competition_name

        self.city = city
        self.country = country

        super().__init__(**kwargs)

    @property
    def summary(self) -> str:
        """ Summary of the Stage event.

        A top-level stage has no session, and "Belgian Grand Prix ()" would be
        a poor calendar entry.
        """
        return f"{self.name} ({self.session})" if self.session else self.name

    @property
    def start(self) -> datetime:
        """ Start time of the Stage event. """
        return self._start if isinstance(self._start, datetime) else datetime.fromisoformat(self._start)

    @property
    def end(self) -> datetime:
        """ End time of the Stage event.

        Stages have real durations that vary a lot — a sprint is half an hour,
        a Grand Prix a couple of hours, a cycling stage most of an afternoon —
        so the provider's own end time is used when it gives one. The two-hour
        fallback only applies when it doesn't.
        """
        if self._end is None:
            return self.start + timedelta(hours=2)
        return self._end if isinstance(self._end, datetime) else datetime.fromisoformat(self._end)

    @property
    def location(self) -> str:
        """ Location of the Stage event.

        Sessions carry a country but usually no city, unlike the weekend they
        belong to. Requiring both left every session entry with a blank
        location while the country sat unused in the description.
        """
        return ", ".join(part for part in (self.city, self.country) if part)

    @property
    def description(self) -> str:
        """ Description of the Stage event. """
        description_lines = [f"Sport: {self.sport}"]
        # "Motorsport" alone doesn't say whether this is F1 or MotoGP, and both
        # run a Netherlands GP. The competition is what disambiguates them.
        if self.competition_name:
            description_lines.append(f"Competition: {self.competition_name}")
        if self.session:
            # The provider's own name is kept when it says something the tier
            # label doesn't — "Stage 14" against a bare "Stage".
            if self.session_name and self.session_name != self.session:
                description_lines.append(f"Session: {self.session} ({self.session_name})")
            else:
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
        """ Factory method to create a StageEvent from a sportindex.StageEvent.

        Stages arrive at two different levels and both have to work:

        - straight from a competition, each stage *is* the weekend ("Belgian
          Grand Prix") and has no parent — sport-index documents `parent` as
          None for a top-level stage;
        - through the sessions filter, each stage is a part of a weekend
          ("Qualifying") whose parent is the weekend itself.

        So the weekend is the name, and the session is the part within it —
        empty when this stage is the weekend.

        The session is labelled from the tier rather than the provider's name,
        which is inconsistent between championships: the same RACE tier is
        "Grand Prix" in F1 and "Race" elsewhere. The provider's name is kept
        alongside it for the cases where it carries more, like "Stage 14".
        """
        parent = event.parent
        return cls(
            start=event.start,
            end=event.end,
            sport=event.sport.name,
            name=parent.name if parent else event.name,
            session=stage_tier_label(event.tier, fallback=event.name) if parent else "",
            session_name=event.name if parent else None,
            competition_name=event.competition.name if event.competition else None,
            city=event.venue.city if event.venue else None,
            country=event.venue.country.name if event.venue and event.venue.country else None,
            source_id=str(event.id),
            sport_idx_event=event
        )

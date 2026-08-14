""" What actually lands in the .ics file.

These cover the gap between "the event object looks right" and "the calendar
entry looks right" — the place where a missing venue became the literal text
"None" in Google Calendar.
"""

from datetime import datetime, timedelta, timezone

from sportindex import StageTier

from sports_calendar.core.calendar import MatchEvent, StageEvent, stage_tier_label

KICKOFF = datetime(2026, 9, 5, 13, 0, tzinfo=timezone.utc)


def _ical(event) -> str:
    return event.get_event().to_ical().decode()


def test_missing_venue_is_omitted_not_written_as_none() -> None:
    """ Rugby fixtures routinely have no venue. """
    event = MatchEvent(
        start=KICKOFF,
        sport="Rugby",
        home_competitor_name="Stade Rochelais",
        away_competitor_name="Stade Toulousain",
        competition_name="France - Top 14",
        venue=None,
    )

    assert event.location == ""
    assert "LOCATION:None" not in _ical(event)
    assert "LOCATION" not in _ical(event)


def test_venue_is_written_when_present() -> None:
    event = MatchEvent(
        start=KICKOFF,
        sport="Football",
        home_competitor_name="RC Lens",
        away_competitor_name="Auxerre",
        venue="Stade Bollaert-Delelis",
    )
    assert "LOCATION:Stade Bollaert-Delelis" in _ical(event)


def test_stage_location_falls_back_to_country_alone() -> None:
    """ Sessions carry a country but no city; the weekend carries both. """
    session = StageEvent(start=KICKOFF, name="Netherlands GP", session="Race", country="Netherlands")
    weekend = StageEvent(start=KICKOFF, name="Netherlands GP", city="Zandvoort", country="Netherlands")

    assert session.location == "Netherlands"
    assert weekend.location == "Zandvoort, Netherlands"


def test_stage_uses_provider_end_when_given() -> None:
    """ A sprint is not two hours long. """
    end = KICKOFF + timedelta(minutes=30)
    assert StageEvent(start=KICKOFF, end=end).end == end


def test_stage_falls_back_to_two_hours_without_an_end() -> None:
    assert StageEvent(start=KICKOFF).end == KICKOFF + timedelta(hours=2)


def test_top_level_stage_summary_has_no_empty_parens() -> None:
    assert StageEvent(start=KICKOFF, name="Netherlands GP").summary == "Netherlands GP"


def test_session_summary_names_the_session() -> None:
    event = StageEvent(start=KICKOFF, name="Netherlands GP", session="Race")
    assert event.summary == "Netherlands GP (Race)"


def test_tier_label_is_preferred_over_the_providers_own_name() -> None:
    """ The same RACE tier is "Grand Prix" in F1 and "Race" elsewhere. """
    assert stage_tier_label(StageTier.RACE, fallback="Grand Prix") == "Race"
    assert stage_tier_label(StageTier.SPRINT_RACE) == "Sprint race"


def test_unlabelled_tier_falls_back_to_the_provider_name() -> None:
    """ Structural tiers have no session label; the raw name beats nothing. """
    assert stage_tier_label(StageTier.EVENT, fallback="Netherlands GP") == "Netherlands GP"
    assert stage_tier_label(None, fallback="Netherlands GP") == "Netherlands GP"


def test_provider_name_is_kept_in_the_description_when_it_says_more() -> None:
    """ "Stage 14" carries a number that the bare tier label loses. """
    event = StageEvent(
        start=KICKOFF, name="Tour de France", session="Stage", session_name="Stage 14",
    )
    assert "Session: Stage (Stage 14)" in event.description


def test_provider_name_is_not_repeated_when_it_matches_the_label() -> None:
    event = StageEvent(start=KICKOFF, name="Netherlands GP", session="Race", session_name="Race")
    assert "Session: Race" in event.description
    assert "(Race)" not in event.description

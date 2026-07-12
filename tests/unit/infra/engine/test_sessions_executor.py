from unittest.mock import Mock

from sportindex import EventCollection, StageEvent, StageTier

from sports_calendar.core.selection import SessionsFilterFields
from sports_calendar.infra.engine.executors import SessionsExecutor


def _stage_event(tier, competition_id=None):
    event = Mock(spec=StageEvent)
    event.tier = tier
    if competition_id is not None:
        event.competition = Mock(id=competition_id)
    return event


def test_fetch_matches_substages_by_tier_not_name():
    """Regression test: fetch() must match on substage.tier, not substage.name."""
    filter_fields = SessionsFilterFields(competition_id="stgc:42", sessions=[StageTier.RACE])

    race_substage = _stage_event(StageTier.RACE)
    race_substage.name = "Race"

    qualifying_substage = _stage_event(StageTier.QUALIFYING)
    qualifying_substage.name = "Race"  # matching legacy name-based bait, wrong tier

    main_event = Mock(spec=StageEvent)
    main_event.substages = [race_substage, qualifying_substage]

    season = Mock()
    season.get_fixtures.return_value = [main_event]
    competition = Mock(seasons=[season])

    client = Mock()
    client.get.return_value = competition

    result = SessionsExecutor.fetch(filter_fields, client)

    assert list(result) == [race_substage]


def test_filter_events_by_sessions_matches_tier_and_competition():
    filter_fields = SessionsFilterFields(competition_id="stgc:42", sessions=[StageTier.RACE, StageTier.SPRINT_RACE])

    keep = _stage_event(StageTier.RACE, competition_id="stgc:42")
    wrong_tier = _stage_event(StageTier.QUALIFYING, competition_id="stgc:42")
    wrong_competition = _stage_event(StageTier.RACE, competition_id="stgc:99")

    events = EventCollection([keep, wrong_tier, wrong_competition])
    result = SessionsExecutor._filter_events_by_sessions(events, filter_fields)

    assert list(result) == [keep]

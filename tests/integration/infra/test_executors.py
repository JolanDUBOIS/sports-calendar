import logging
from unittest.mock import Mock

import pytest
from sportindex import Event, EventCollection, SportClient, StageTier

from sports_calendar.core.selection import (
    CompetitionsFilterFields,
    CompetitorsFilterFields,
    EmptyFilterFields,
    EntitySelectionRule,
    MinRankingFilterFields,
    Rule,
    SessionsFilterFields,
)
from sports_calendar.infra.engine.executors import (
    CompetitionsExecutor,
    CompetitorsExecutor,
    EmptyExecutor,
    MinRankingExecutor,
    SessionsExecutor,
)

logger = logging.getLogger(__name__)


# ==== Constants ====

PSG_ID = "t-cpt:1644"
ANY_SELECTION_RULE = EntitySelectionRule(rule=Rule.ANY)
BOTH_SELECTION_RULE = EntitySelectionRule(rule=Rule.BOTH)
OPPONENT_SELECTION_RULE = EntitySelectionRule(rule=Rule.OPPONENT, reference=PSG_ID)


# ==== Fixtures ====

@pytest.fixture
def sport_client():
    """Provides a fresh, perfectly configured SportClient for each test."""
    return SportClient()

@pytest.fixture
def mock_event_collection():
    mock_event_1 = Mock(spec=Event)
    mock_event_1.id = 1

    mock_event_2 = Mock(spec=Event)
    mock_event_2.id = 2

    events = EventCollection([mock_event_1, mock_event_2])
    return events


# ==== Helper Functions ====

def is_competitor_in_teams(event: Event, teams: list[str], side: str = "home") -> bool:
    if side == "home":
        return event.competitors.home.id in teams
    elif side == "away":
        return event.competitors.away.id in teams
    elif side == "both":
        return event.competitors.home.id in teams and event.competitors.away.id in teams
    elif side == "any":
        return event.competitors.home.id in teams or event.competitors.away.id in teams
    else:
        raise ValueError("Invalid side argument")


# ==== Test Cases ====

def test_empty_executor_fetch(sport_client):
    filter_fields = EmptyFilterFields()
    result = EmptyExecutor.fetch(filter_fields, sport_client)

    assert isinstance(result, EventCollection)
    assert len(result) == 0


def test_empty_executor_apply(mock_event_collection, sport_client):
    filter_fields = EmptyFilterFields()
    result = EmptyExecutor.apply(filter_fields, mock_event_collection, sport_client)

    assert isinstance(result, EventCollection)
    assert len(result) == 2


def test_competitions_executor_fetch(football_competition_ids, sport_client):
    filter_fields = CompetitionsFilterFields(competition_ids=football_competition_ids)
    result = CompetitionsExecutor.fetch(filter_fields, sport_client)

    if len(result) == 0:
        logger.warning("No events fetched for competitions filter.")

    assert isinstance(result, EventCollection)
    assert all(event.competition.id in football_competition_ids for event in result)


def test_competitions_executor_apply(football_competition_ids, sport_client):
    # We assume test_competitions_executor_fetch passed
    filter_fields = CompetitionsFilterFields(competition_ids=football_competition_ids)
    all_events = CompetitionsExecutor.fetch(filter_fields, sport_client)

    if len(all_events) == 0:
        logger.warning("No events available to apply competitions filter.")

    new_filter_fields = CompetitionsFilterFields(competition_ids=football_competition_ids[:2])
    result = CompetitionsExecutor.apply(new_filter_fields, all_events, sport_client)

    if len(result) == 0:
        logger.warning("No events after applying competitions filter with reduced competition list.")

    assert isinstance(result, EventCollection)
    assert all(event.competition.id in football_competition_ids[:2] for event in result)
    assert len(result) <= len(all_events)


def test_competitors_executor_fetch(football_team_ids, sport_client):
    filter_fields_any = CompetitorsFilterFields(competitor_ids=football_team_ids, selection_rule=ANY_SELECTION_RULE)
    filter_fields_both = CompetitorsFilterFields(competitor_ids=football_team_ids, selection_rule=BOTH_SELECTION_RULE)
    filter_fields_opponent = CompetitorsFilterFields(competitor_ids=football_team_ids, selection_rule=OPPONENT_SELECTION_RULE)

    result_any = CompetitorsExecutor.fetch(filter_fields_any, sport_client)
    result_both = CompetitorsExecutor.fetch(filter_fields_both, sport_client)
    result_opponent = CompetitorsExecutor.fetch(filter_fields_opponent, sport_client)

    if len(result_any) == 0:
        logger.warning("No events fetched for competitors filter with ANY selection rule.")
    if len(result_both) == 0:
        logger.warning("No events fetched for competitors filter with BOTH selection rule.")
    if len(result_opponent) == 0:
        logger.warning("No events fetched for competitors filter with OPPONENT selection rule.")

    assert isinstance(result_any, EventCollection)
    assert isinstance(result_both, EventCollection)
    assert isinstance(result_opponent, EventCollection)

    assert all(is_competitor_in_teams(event, football_team_ids, side="any") for event in result_any)
    assert all(is_competitor_in_teams(event, football_team_ids, side="both") for event in result_both)
    assert all(is_competitor_in_teams(event, [PSG_ID], side="any") for event in result_opponent)
    assert all(is_competitor_in_teams(event, football_team_ids + [PSG_ID], side="both") for event in result_opponent)


def test_competitors_executor_apply(football_team_ids, sport_client):
    # We assume test_competitors_executor_fetch passed
    filter_fields = CompetitorsFilterFields(competitor_ids=football_team_ids, selection_rule=ANY_SELECTION_RULE)
    all_events = CompetitorsExecutor.fetch(filter_fields, sport_client)

    if len(all_events) == 0:
        logger.warning("No events available to apply competitors filter.")

    new_filter_fields_any = CompetitorsFilterFields(competitor_ids=football_team_ids[:6], selection_rule=ANY_SELECTION_RULE)
    new_filter_fields_both = CompetitorsFilterFields(competitor_ids=football_team_ids[:6], selection_rule=BOTH_SELECTION_RULE)
    new_filter_fields_opponent = CompetitorsFilterFields(competitor_ids=football_team_ids[:6], selection_rule=OPPONENT_SELECTION_RULE)

    result_any = CompetitorsExecutor.apply(new_filter_fields_any, all_events, sport_client)
    result_both = CompetitorsExecutor.apply(new_filter_fields_both, all_events, sport_client)
    result_opponent = CompetitorsExecutor.apply(new_filter_fields_opponent, all_events, sport_client)

    if len(result_any) == 0:
        logger.warning("No events after applying competitors filter with ANY selection rule.")
    if len(result_both) == 0:
        logger.warning("No events after applying competitors filter with BOTH selection rule.")
    if len(result_opponent) == 0:
        logger.warning("No events after applying competitors filter with OPPONENT selection rule.")

    assert isinstance(result_any, EventCollection)
    assert isinstance(result_both, EventCollection)
    assert isinstance(result_opponent, EventCollection)

    assert len(result_any) <= len(all_events)
    assert len(result_both) <= len(all_events)
    assert len(result_opponent) <= len(all_events)

    assert all(is_competitor_in_teams(event, football_team_ids[:6], side="any") for event in result_any)
    assert all(is_competitor_in_teams(event, football_team_ids[:6], side="both") for event in result_both)
    assert all(is_competitor_in_teams(event, [PSG_ID], side="any") for event in result_opponent)
    assert all(is_competitor_in_teams(event, football_team_ids[:6] + [PSG_ID], side="both") for event in result_opponent)


def test_min_ranking_executor_fetch(football_competition_ids, sport_client):
    filter_fields_any = MinRankingFilterFields(ranking=10, competition_ids=football_competition_ids[1:3], selection_rule=ANY_SELECTION_RULE)
    filter_fields_both = MinRankingFilterFields(ranking=10, competition_ids=football_competition_ids[1:3], selection_rule=BOTH_SELECTION_RULE)
    filter_fields_opponent = MinRankingFilterFields(ranking=10, competition_ids=football_competition_ids[1:3], selection_rule=OPPONENT_SELECTION_RULE)

    result_any = MinRankingExecutor.fetch(filter_fields_any, sport_client)
    result_both = MinRankingExecutor.fetch(filter_fields_both, sport_client)
    result_opponent = MinRankingExecutor.fetch(filter_fields_opponent, sport_client)

    if len(result_any) == 0:
        logger.warning("No events fetched for minimum ranking filter with ANY selection rule.")
    if len(result_both) == 0:
        logger.warning("No events fetched for minimum ranking filter with BOTH selection rule.") # This is very likely if the test is realized at a random time during the season...
    if len(result_opponent) == 0:
        logger.warning("No events fetched for minimum ranking filter with OPPONENT selection rule.") # This is even more likely if the test is realized at a random time during the season...

    assert isinstance(result_any, EventCollection)
    assert isinstance(result_both, EventCollection)
    assert isinstance(result_opponent, EventCollection)

    for event in result_any:
        if event.competition.id in football_competition_ids:
            total_standings = next((s for s in event.competition.seasons[0].standings if s.kind == "total"), None)
            competition_standings = total_standings.entries
            top_10_team_ids = {entry.competitor.id for entry in competition_standings if entry.position <= 10}
            assert is_competitor_in_teams(event, top_10_team_ids, side="any"), f"Event {event.id} with competitors {event.competitors.home.id} vs {event.competitors.away.id} does not meet minimum ranking criteria for ANY selection rule: {top_10_team_ids}"


def test_min_ranking_executor_apply(football_competition_ids, sport_client):
    # We assume test_competitions_executor_fetch passed
    filter_fields = CompetitionsFilterFields(competition_ids=football_competition_ids[1:3])
    all_events = CompetitionsExecutor.fetch(filter_fields, sport_client)

    if len(all_events) == 0:
        logger.warning("No events available to apply minimum ranking filter.")

    new_filter_fields_anyy = MinRankingFilterFields(ranking=10, competition_ids=football_competition_ids[1:3], selection_rule=ANY_SELECTION_RULE)
    new_filter_fields_both = MinRankingFilterFields(ranking=10, competition_ids=football_competition_ids[1:3], selection_rule=BOTH_SELECTION_RULE)
    new_filter_fields_opponent = MinRankingFilterFields(ranking=10, competition_ids=football_competition_ids[1:3], selection_rule=OPPONENT_SELECTION_RULE)

    result_any = MinRankingExecutor.apply(new_filter_fields_anyy, all_events, sport_client)
    result_both = MinRankingExecutor.apply(new_filter_fields_both, all_events, sport_client)
    result_opponent = MinRankingExecutor.apply(new_filter_fields_opponent, all_events, sport_client)

    if len(result_any) == 0:
        logger.warning("No events after applying minimum ranking filter with ANY selection rule.")
    if len(result_both) == 0:
        logger.warning("No events after applying minimum ranking filter with BOTH selection rule.")
    if len(result_opponent) == 0:
        logger.warning("No events after applying minimum ranking filter with OPPONENT selection rule.")

    assert isinstance(result_any, EventCollection)
    assert isinstance(result_both, EventCollection)
    assert isinstance(result_opponent, EventCollection)

    assert len(result_any) <= len(all_events)
    assert len(result_both) <= len(all_events)
    assert len(result_opponent) <= len(all_events)


def test_sessions_executor_fetch(sport_client):
    filter_fields = SessionsFilterFields(competition_id="stgc:40", sessions=[StageTier.QUALIFYING, StageTier.SPRINT_RACE, StageTier.RACE])
    result = SessionsExecutor.fetch(filter_fields, sport_client)

    if len(result) == 0:
        logger.warning("No events fetched for sessions filter.") # Might happen if the test is realized in the off-season for instance...

    assert isinstance(result, EventCollection)
    assert all(event.competition.id == "stgc:40" for event in result)
    assert all(event.tier in filter_fields.sessions for event in result)


def test_sessions_executor_apply(sport_client):
    filter_fields = SessionsFilterFields(competition_id="stgc:40", sessions=[StageTier.QUALIFYING, StageTier.SPRINT_RACE, StageTier.RACE])
    all_events = SessionsExecutor.fetch(filter_fields, sport_client)

    if len(all_events) == 0:
        logger.warning("No events available to apply sessions filter.")

    new_filter_fields = SessionsFilterFields(competition_id="stgc:40", sessions=[StageTier.RACE])
    result = SessionsExecutor.apply(new_filter_fields, all_events, sport_client)

    if len(result) == 0:
        logger.warning("No events after applying sessions filter with reduced session list.")

    assert isinstance(result, EventCollection)
    assert all(event.competition.id == "stgc:40" for event in result)
    assert all(event.tier == StageTier.RACE for event in result)
    assert len(result) <= len(all_events)

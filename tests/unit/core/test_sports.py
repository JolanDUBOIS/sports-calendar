""" The supported-sport set and the per-sport filter capability table.

These guard a failure mode that is invisible at runtime: offering a filter a
sport cannot satisfy does not raise, it just yields an empty calendar.
"""

from sports_calendar.core.calendar import EVENT_TYPE_MAP
from sports_calendar.core.selection import FilterType
from sports_calendar.core.sports import (
    BASKETBALL,
    CYCLING,
    FOOTBALL,
    HANDBALL,
    MOTORSPORT,
    RUGBY,
    SUPPORTED_SPORT_IDS,
    TENNIS,
    allowed_filter_types,
    is_supported,
    ranking_choices,
)

THE_SEVEN = {FOOTBALL, BASKETBALL, TENNIS, HANDBALL, MOTORSPORT, RUGBY, CYCLING}


def test_supported_sports_are_exactly_the_seven_we_target() -> None:
    assert SUPPORTED_SPORT_IDS == THE_SEVEN


def test_supported_set_is_derived_from_the_event_map() -> None:
    """ A sport is supported iff its fixtures can become calendar events.

    If these ever disagree, `build_calendar` raises for a sport the UI offered.
    """
    assert SUPPORTED_SPORT_IDS == set(EVENT_TYPE_MAP)


def test_unsupported_sport_is_rejected() -> None:
    assert not is_supported(4)  # ice-hockey: no event class
    assert is_supported(FOOTBALL)


def test_sessions_only_offered_for_staged_sports() -> None:
    """ SessionsExecutor skips anything that is not a StageEvent. """
    for sport_id in (MOTORSPORT, CYCLING):
        assert FilterType.SESSIONS in allowed_filter_types(sport_id)

    for sport_id in (FOOTBALL, BASKETBALL, TENNIS, HANDBALL, RUGBY):
        assert FilterType.SESSIONS not in allowed_filter_types(sport_id)


def test_min_ranking_only_offered_where_a_league_table_exists() -> None:
    """ MinRankingExecutor reads competition standings, which tennis lacks. """
    for sport_id in (FOOTBALL, BASKETBALL, HANDBALL, RUGBY):
        assert FilterType.MIN_RANKING in allowed_filter_types(sport_id)

    for sport_id in (TENNIS, MOTORSPORT, CYCLING):
        assert FilterType.MIN_RANKING not in allowed_filter_types(sport_id)


def test_world_ranking_offered_where_a_governing_body_publishes_one() -> None:
    """ Tennis has no league table but does have ATP/WTA — the whole point. """
    for sport_id in (TENNIS, FOOTBALL, RUGBY):
        assert FilterType.WORLD_RANKING in allowed_filter_types(sport_id)

    for sport_id in (BASKETBALL, HANDBALL, MOTORSPORT, CYCLING):
        assert FilterType.WORLD_RANKING not in allowed_filter_types(sport_id)


def test_every_sport_offering_world_ranking_has_rankings_to_choose_from() -> None:
    """ Offering the filter with an empty dropdown would be a dead end. """
    for sport_id in THE_SEVEN:
        offered = FilterType.WORLD_RANKING in allowed_filter_types(sport_id)
        assert offered == bool(ranking_choices(sport_id)), f"sport {sport_id} disagrees"


def test_ranking_choices_carry_the_provider_ids() -> None:
    assert ranking_choices(TENNIS) == ((5, "ATP (men)"), (6, "WTA (women)"))
    assert dict(ranking_choices(FOOTBALL))[2] == "FIFA World Ranking"
    assert dict(ranking_choices(RUGBY))[3] == "Rugby Union"
    assert ranking_choices(MOTORSPORT) == ()


def test_uefa_countries_is_not_offered_as_a_competitor_ranking() -> None:
    """ It ranks leagues, not teams: its top five are the Premier League,
    Serie A, LaLiga, Bundesliga and Ligue 1. As a competitor filter it can only
    ever resolve to nothing. """
    assert 1 not in dict(ranking_choices(FOOTBALL))


def test_competitions_and_competitors_work_everywhere() -> None:
    for sport_id in THE_SEVEN:
        allowed = allowed_filter_types(sport_id)
        assert FilterType.COMPETITIONS in allowed
        assert FilterType.COMPETITORS in allowed


def test_empty_is_always_allowed() -> None:
    """ Every filter is created before it is configured. """
    for sport_id in (*THE_SEVEN, 4, 999):
        assert FilterType.EMPTY in allowed_filter_types(sport_id)


def test_unknown_sport_falls_back_to_the_safe_subset() -> None:
    """ An unmapped sport gets only what works everywhere, never a silent no-op. """
    allowed = allowed_filter_types(999)
    assert allowed == {FilterType.COMPETITIONS, FilterType.COMPETITORS, FilterType.EMPTY}

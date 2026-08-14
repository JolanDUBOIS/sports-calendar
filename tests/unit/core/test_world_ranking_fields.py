""" Serialisation of the world-ranking filter.

Filters are persisted to YAML and read back, so a field that survives creation
but not a round-trip breaks silently on the next app start.
"""

import pytest

from sports_calendar.core.selection import (
    EntitySelectionRule,
    FilterType,
    Rule,
    WorldRankingFilterFields,
)


def _fields(**overrides) -> WorldRankingFilterFields:
    defaults = {"ranking": 20, "ranking_id": 5, "sport_id": 5}
    return WorldRankingFilterFields(**{**defaults, **overrides})


def test_round_trips_through_a_dict() -> None:
    original = _fields(selection_rule=EntitySelectionRule(rule=Rule.ANY))
    restored = WorldRankingFilterFields.from_dict(original.to_dict())

    assert restored.ranking == 20
    assert restored.ranking_id == 5
    assert restored.sport_id == 5
    assert restored.selection_rule.rule is Rule.ANY


def test_sport_id_survives_the_round_trip() -> None:
    """ Rankings are only reachable through their sport, so losing this id
    would leave the executor unable to find the table at all. """
    assert "sport_id" in _fields().to_dict()
    assert WorldRankingFilterFields.from_dict(_fields().to_dict()).sport_id == 5


def test_rejects_another_filter_types_payload() -> None:
    payload = _fields().to_dict() | {"filter_type": FilterType.MIN_RANKING.value}
    with pytest.raises(ValueError, match="Invalid filter_type"):
        WorldRankingFilterFields.from_dict(payload)


def test_rejects_a_non_positive_cut_off() -> None:
    with pytest.raises(ValueError):
        _fields(ranking=0)


def test_clone_is_independent() -> None:
    original = _fields(selection_rule=EntitySelectionRule(rule=Rule.ANY))
    clone = original.clone()
    clone.selection_rule.rule = Rule.BOTH

    assert original.selection_rule.rule is Rule.ANY

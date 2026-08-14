""" Narrowing a competitions filter to "this round and everything after".

Ordering is each competition's own — sport-index returns rounds in the order
they are played — so one choice resolves correctly in every competition without
any ranking of ours.
"""

from types import SimpleNamespace

import pytest
from sportindex import EventCollection

from sports_calendar.core.rounds import GROUP_PHASE_SLUG
from sports_calendar.core.selection import CompetitionsFilterFields
from sports_calendar.infra.engine.executors import CompetitionsExecutor

UCL = "trnc:7"
FA_CUP = "trnc:19"

# The Champions League ladder as the provider reports it.
UCL_LADDER = {
    "playoff-round": "Playoff round",
    GROUP_PHASE_SLUG: "Group phase",
    "round-of-16": "Round of 16",
    "quarterfinals": "Quarterfinals",
    "semifinals": "Semifinals",
    "final": "Final",
}
# The FA Cup names its early rounds differently but shares the late ones.
FA_CUP_LADDER = {
    "round-4": "Round 4",
    "round-5": "Round 5",
    "quarterfinals": "Quarterfinals",
    "semifinals": "Semifinals",
    "final": "Final",
}
LADDERS = {UCL: UCL_LADDER, FA_CUP: FA_CUP_LADDER}


@pytest.fixture(autouse=True)
def _stub_provider(monkeypatch):
    """ EventCollection admits only real provider entities, and the executor
    would otherwise fetch each competition's rounds over the network. """
    monkeypatch.setattr(EventCollection, "_validate_item", lambda self, item: None)
    monkeypatch.setattr(
        CompetitionsExecutor,
        "_ordered_rounds",
        staticmethod(lambda competition_id, client: LADDERS.get(competition_id, {})),
    )


def _event(competition_id: str, round_slug: str | None, name: str = "m", *, has_round: bool = True):
    """ Three shapes: a named round, an unnamed group-phase round, no round. """
    if not has_round:
        round_ = None
    elif round_slug:
        round_ = SimpleNamespace(slug=round_slug, name=round_slug, value=1)
    else:
        round_ = SimpleNamespace(slug=None, name=None, value=1)

    return SimpleNamespace(
        id=f"mch:{name}", name=name, round=round_,
        competition=SimpleNamespace(id=competition_id),
    )


def _apply(events: list, competition_ids: list[str], from_round: str | None) -> list:
    collection = EventCollection()
    for event in events:
        collection.add(event)
    fields = CompetitionsFilterFields(competition_ids=competition_ids, from_round=from_round)
    return sorted(e.name for e in CompetitionsExecutor.apply(fields, collection, client=None))


def test_no_round_chosen_keeps_the_whole_competition() -> None:
    events = [_event(UCL, "final", "a"), _event(UCL, None, "b")]
    assert _apply(events, [UCL], from_round=None) == ["a", "b"]


def test_from_a_round_keeps_it_and_everything_after() -> None:
    events = [
        _event(UCL, "round-of-16", "r16"),
        _event(UCL, "quarterfinals", "qf"),
        _event(UCL, "semifinals", "sf"),
        _event(UCL, "final", "f"),
    ]
    assert _apply(events, [UCL], from_round="quarterfinals") == ["f", "qf", "sf"]


def test_earlier_rounds_are_dropped() -> None:
    events = [_event(UCL, "playoff-round", "po"), _event(UCL, "final", "f")]
    assert _apply(events, [UCL], from_round="final") == ["f"]


def test_from_the_group_phase_keeps_the_knockouts_too() -> None:
    """ The group phase sits before the knockouts, so "from" it means all of it. """
    events = [
        _event(UCL, "playoff-round", "po"),
        _event(UCL, None, "group"),
        _event(UCL, "final", "f"),
    ]
    assert _apply(events, [UCL], from_round=GROUP_PHASE_SLUG) == ["f", "group"]


def test_group_phase_matches_are_dropped_when_starting_later() -> None:
    events = [_event(UCL, None, "group"), _event(UCL, "final", "f")]
    assert _apply(events, [UCL], from_round="quarterfinals") == ["f"]


def test_the_same_choice_resolves_per_competition() -> None:
    """ "From the quarter-finals" in two ladders that differ earlier on. """
    events = [
        _event(UCL, "round-of-16", "ucl-r16"),
        _event(UCL, "final", "ucl-final"),
        _event(FA_CUP, "round-5", "fa-r5"),
        _event(FA_CUP, "semifinals", "fa-sf"),
    ]
    assert _apply(events, [UCL, FA_CUP], from_round="quarterfinals") == ["fa-sf", "ucl-final"]


def test_a_competition_without_the_chosen_round_contributes_nothing() -> None:
    """ "From the round of 16" has no meaning in the FA Cup's ladder. """
    events = [_event(UCL, "final", "ucl"), _event(FA_CUP, "final", "fa")]
    assert _apply(events, [UCL, FA_CUP], from_round="round-of-16") == ["ucl"]


def test_events_without_any_round_are_dropped() -> None:
    events = [_event(UCL, None, "a", has_round=False), _event(UCL, "final", "b")]
    assert _apply(events, [UCL], from_round=GROUP_PHASE_SLUG) == ["b"]


def test_competition_matching_survives_the_two_id_views() -> None:
    events = [_event("trnc:7", "final", "a")]
    assert _apply(events, ["t-trnc:7"], from_round=None) == ["a"]

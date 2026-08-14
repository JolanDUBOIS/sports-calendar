""" An item is the union of its filters, not their intersection.

Filters get named "All PSG games", "Supercups", "Finals of the Europa League" —
a list of things to follow. Intersecting them emptied the calendar as soon as
two of them named different competitions, which is to say almost always.
"""

from dataclasses import dataclass

import pytest
from sportindex import EventCollection, SportClient

from sports_calendar.core.selection import (
    CompetitionsFilterFields,
    CompetitorsFilterFields,
    EmptyFilterFields,
    Selection,
    SelectionFilter,
    SelectionItem,
)
from sports_calendar.infra.engine import resolver as resolver_module
from sports_calendar.infra.engine.resolver import Resolver

FOOTBALL = 1


@pytest.fixture(autouse=True)
def _accept_stub_events(monkeypatch):
    """ EventCollection admits only real provider entities. """
    monkeypatch.setattr(EventCollection, "_validate_item", lambda self, item: None)


@dataclass(frozen=True)
class _Event:
    """ Hashed by id, as `IdentifiableEntity` is — which is what lets the union
    carry a fixture once when two filters both name it. """

    id: str
    name: str


def _event(name: str) -> _Event:
    return _Event(id=f"mch:{name}", name=name)


def _collection(*names: str) -> EventCollection:
    collection = EventCollection()
    for name in names:
        collection.add(_event(name))
    return collection


class _StubExecutor:
    """ Returns a canned collection, and records whether `apply` was reached. """

    returns: dict = {}
    applied: list = []

    @classmethod
    def fetch(cls, filter_fields, client):
        return cls.returns.get(id(filter_fields), EventCollection())

    @classmethod
    def apply(cls, filter_fields, events, client):
        cls.applied.append(filter_fields)
        return EventCollection()


@pytest.fixture
def executor_map(monkeypatch):
    """ Every filter type resolves through the same stub. """
    _StubExecutor.returns = {}
    _StubExecutor.applied = []

    class _EveryType(dict):
        def get(self, _key, _default=None):
            return _StubExecutor

    monkeypatch.setattr(resolver_module, "EXECUTOR_MAP", _EveryType())
    return _StubExecutor


def _filter(fields) -> SelectionFilter:
    return SelectionFilter(sport_id=FOOTBALL, fields=fields)


def _resolve(item: SelectionItem) -> list[str]:
    client = SportClient()
    events = Resolver._resolve_item(item, client)
    return sorted(event.name for event in events)


def test_disjoint_filters_are_merged_not_intersected(executor_map) -> None:
    """ The case that silently emptied the real selection.

    Two competitions filters naming different competitions have nothing in
    common, so intersecting them yielded nothing at all.
    """
    ucl = CompetitionsFilterFields(competition_ids=["trnc:7"])
    supercups = CompetitionsFilterFields(competition_ids=["trnc:339"])
    executor_map.returns = {
        id(ucl): _collection("ucl-final"),
        id(supercups): _collection("supercup"),
    }

    item = SelectionItem(sport_id=FOOTBALL, filters=[_filter(ucl), _filter(supercups)])
    assert _resolve(item) == ["supercup", "ucl-final"]


def test_order_does_not_change_the_result(executor_map) -> None:
    """ Which is what makes reordering filters safe to expose in the UI. """
    first = CompetitionsFilterFields(competition_ids=["trnc:7"])
    second = CompetitorsFilterFields(competitor_ids=["team:1644"])
    executor_map.returns = {id(first): _collection("a"), id(second): _collection("b")}

    forwards = SelectionItem(sport_id=FOOTBALL, filters=[_filter(first), _filter(second)])
    backwards = SelectionItem(sport_id=FOOTBALL, filters=[_filter(second), _filter(first)])
    assert _resolve(forwards) == _resolve(backwards) == ["a", "b"]


def test_an_empty_filter_no_longer_erases_the_item(executor_map) -> None:
    """ `EmptyExecutor.fetch` yields nothing, which used to seed the chain. """
    empty = EmptyFilterFields()
    real = CompetitionsFilterFields(competition_ids=["trnc:7"])
    executor_map.returns = {id(real): _collection("ucl-final")}

    item = SelectionItem(sport_id=FOOTBALL, filters=[_filter(empty), _filter(real)])
    assert _resolve(item) == ["ucl-final"]


def test_a_fixture_named_twice_is_carried_once(executor_map) -> None:
    """ PSG's Champions League tie, followed as a team and as a competition. """
    by_competition = CompetitionsFilterFields(competition_ids=["trnc:7"])
    by_team = CompetitorsFilterFields(competitor_ids=["team:1644"])
    executor_map.returns = {
        id(by_competition): _collection("psg-v-real", "bayern-v-inter"),
        id(by_team): _collection("psg-v-real"),
    }

    item = SelectionItem(sport_id=FOOTBALL, filters=[_filter(by_competition), _filter(by_team)])
    assert _resolve(item) == ["bayern-v-inter", "psg-v-real"]


def test_no_filter_is_ever_narrowed_by_another(executor_map) -> None:
    """ `apply` is still the executors' own tool, but the resolver never calls it. """
    fields = CompetitionsFilterFields(competition_ids=["trnc:7"])
    executor_map.returns = {id(fields): _collection("a")}

    _resolve(SelectionItem(sport_id=FOOTBALL, filters=[_filter(fields), _filter(fields)]))
    assert executor_map.applied == []


def test_an_item_with_no_filters_resolves_to_nothing(executor_map) -> None:
    assert _resolve(SelectionItem(sport_id=FOOTBALL, filters=[])) == []


def test_items_stay_separate_collections(executor_map) -> None:
    """ Union within an item; one collection per item, tagged with its sport. """
    football = CompetitionsFilterFields(competition_ids=["trnc:7"])
    tennis = CompetitorsFilterFields(competitor_ids=["p-cpt:1"])
    executor_map.returns = {id(football): _collection("ucl"), id(tennis): _collection("wimbledon")}

    selection = Selection(
        name="test",
        items=[
            SelectionItem(sport_id=FOOTBALL, filters=[_filter(football)]),
            SelectionItem(sport_id=5, filters=[_filter(tennis)]),
        ],
    )
    resolved = Resolver.resolve_selection(selection, SportClient())
    assert [(sorted(e.name for e in events), sport_id) for events, sport_id in resolved] == [
        (["ucl"], FOOTBALL),
        (["wimbledon"], 5),
    ]

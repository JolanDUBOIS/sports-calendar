""" One unreadable session must not cost the whole calendar.

`substage.tier` is lazy — reading it fetches the substage — so a single flaky
request raised straight out of `resolve_events` and lost every event, across
every sport, for every other rule. Seen for real: one 3-attempt failure on
`/api/v1/stage/214277` took down a build of 129 events.
"""

import pytest
from sportindex import EventCollection, StageEvent, StageTier
from sportindex.exceptions import DomainError, FetchError

from sports_calendar.core.selection import SessionsFilterFields
from sports_calendar.infra.engine.executors import SessionsExecutor

RACE, SPRINT = StageTier(6), StageTier(10)
COMPETITION = "stgc:40"


class _Stub(StageEvent):
    """ A stage that never touches the provider.

    A real `StageEvent` is constructed from fetched data; `object.__new__`
    sidesteps that while keeping the `isinstance(event, StageEvent)` check the
    executor relies on honest.
    """

    def __new__(cls, *args, **kwargs):  # noqa: ARG004
        return object.__new__(cls)

    def __init__(self, identifier, tier=None, error=None, substages=None, substages_error=None):
        self._id = identifier
        self._tier = tier
        self._error = error
        self._substages = substages or []
        self._substages_error = substages_error

    @property
    def id(self):
        return self._id

    @property
    def tier(self):
        if self._error is not None:
            raise self._error
        return self._tier

    @property
    def substages(self):
        if self._substages_error is not None:
            raise self._substages_error
        return self._substages


@pytest.fixture(autouse=True)
def _accept_stubs(monkeypatch):
    monkeypatch.setattr(EventCollection, "_validate_item", lambda self, item: None)


def _run(monkeypatch, parent: _Stub):
    fixtures = EventCollection()
    fixtures.add(parent)
    monkeypatch.setattr(
        SessionsExecutor, "fetch_current_fixtures",
        staticmethod(lambda competition_id, client: fixtures),
    )
    fields = SessionsFilterFields(competition_id=COMPETITION, sessions=[RACE, SPRINT])
    return SessionsExecutor.fetch(fields, client=None)


def test_a_session_that_will_not_load_is_skipped(monkeypatch) -> None:
    parent = _Stub("stg:parent", substages=[
        _Stub("stg:1", tier=RACE),
        _Stub("stg:2", error=DomainError("network error while fetching stage")),
        _Stub("stg:3", tier=SPRINT),
    ])
    assert sorted(e.id for e in _run(monkeypatch, parent)) == ["stg:1", "stg:3"]


def test_a_fetch_error_is_survived_too(monkeypatch) -> None:
    parent = _Stub("stg:parent", substages=[
        _Stub("stg:1", tier=RACE),
        _Stub("stg:2", error=FetchError("failed after 3 attempts")),
    ])
    assert [e.id for e in _run(monkeypatch, parent)] == ["stg:1"]


def test_a_stage_whose_session_list_fails_is_skipped_whole(monkeypatch) -> None:
    parent = _Stub("stg:parent", substages_error=DomainError("boom"))
    assert list(_run(monkeypatch, parent)) == []


def test_sessions_not_asked_for_are_still_excluded(monkeypatch) -> None:
    """ The guard must not quietly become "keep everything". """
    parent = _Stub("stg:parent", substages=[
        _Stub("stg:1", tier=RACE),
        _Stub("stg:9", tier=StageTier(3)),
    ])
    assert [e.id for e in _run(monkeypatch, parent)] == ["stg:1"]

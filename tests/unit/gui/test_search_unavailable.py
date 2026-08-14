""" An unreachable provider must not look like an empty result.

Both render as "nothing found" in the search box, and the difference is whether
the user should correct their spelling or turn off their VPN. Diagnosing the
second took an evening with the source code open; a beta user has no chance.
"""

import pytest
from sportindex import Competition, Competitor, SportClient
from sportindex.exceptions import (
    EntityNotFoundError,
    FetchError,
    NetworkError,
    RateLimitError,
)

from sports_calendar.interfaces.gui.catalog import (
    SearchUnavailableError,
    SportIndexFilterSearchProvider,
)

FOOTBALL = 1


def _provider(monkeypatch, raises=None, returns=()):
    def fake_search(self, entity_cls, **kwargs):  # noqa: ARG001
        if raises is not None:
            raise raises
        return returns

    monkeypatch.setattr(SportClient, "search", fake_search)
    return SportIndexFilterSearchProvider(SportClient())


@pytest.mark.parametrize("error", [
    NetworkError("connection refused"),
    FetchError("could not fetch"),
    RateLimitError("slow down"),
])
def test_unreachable_provider_is_reported_not_swallowed(monkeypatch, error) -> None:
    provider = _provider(monkeypatch, raises=error)

    with pytest.raises(SearchUnavailableError):
        provider.search_competition("champions", FOOTBALL)
    with pytest.raises(SearchUnavailableError):
        provider.search_competitor("alcaraz", FOOTBALL)


def test_a_provider_answer_of_none_is_not_a_network_failure(monkeypatch) -> None:
    """ A search that legitimately matched nothing stays an empty result. """
    provider = _provider(monkeypatch, returns=[])
    assert provider.search_competition("zzzzz", FOOTBALL) == {}


def test_other_provider_errors_are_not_disguised_as_unreachable(monkeypatch) -> None:
    """ Only failures to reach anyone. A domain error is a different bug and
    must not be reported to the user as "check your VPN". """
    provider = _provider(monkeypatch, raises=EntityNotFoundError("no such thing"))

    with pytest.raises(EntityNotFoundError):
        provider.search_competition("champions", FOOTBALL)


def test_results_are_still_filtered_to_the_requested_sport(monkeypatch) -> None:
    """ The refactor that introduced SearchUnavailableError merged two near-identical
    search methods; this is the behaviour they shared. """
    from types import SimpleNamespace

    football = SimpleNamespace(id="trnc:7", name="UCL", sport=SimpleNamespace(id="spt:1"))
    tennis = SimpleNamespace(id="trnc:2363", name="AO", sport=SimpleNamespace(id="spt:5"))
    provider = _provider(monkeypatch, returns=[football, tennis])

    assert provider.search_competition("a", FOOTBALL) == {"trnc:7": "UCL"}


def test_competitor_and_competition_searches_ask_for_their_own_type(monkeypatch) -> None:
    asked: list[type] = []

    def fake_search(self, entity_cls, **kwargs):  # noqa: ARG001
        asked.append(entity_cls)
        return []

    monkeypatch.setattr(SportClient, "search", fake_search)
    provider = SportIndexFilterSearchProvider(SportClient())

    provider.search_competition("a", FOOTBALL)
    provider.search_competitor("b", FOOTBALL)
    assert asked == [Competition, Competitor]

""" Remembering entity names across runs.

A rule stores ids, and a card shows names, so drawing a card used to mean a
round trip per team every time the app started — and when the provider was
unreachable the name fell back to the raw id, so the same card read "PSG" one
minute and "trnc:16" the next.

What matters here is what must *not* happen: a failed lookup must never be
remembered as if it were a name, and a cache that cannot be read or written
must slow the app down rather than break it.
"""

import json
import pathlib
from types import SimpleNamespace

import pytest

from sports_calendar.infra.config.paths import Paths
from sports_calendar.interfaces.gui import catalog


class _Client:
    """ A provider that counts its calls and can be told to fail. """

    def __init__(self, entities: dict, *, unreachable: bool = False):
        self._entities = entities
        self.unreachable = unreachable
        self.calls = 0

    def get(self, entity_id, entity_cls):  # noqa: ARG002
        self.calls += 1
        if self.unreachable:
            raise ConnectionError("provider unreachable")
        return self._entities.get(str(entity_id))


@pytest.fixture(autouse=True)
def cache_file(tmp_path, monkeypatch):
    """ Point the cache at a scratch file and empty it between tests. """
    path = tmp_path / "entity-labels.json"
    monkeypatch.setattr(Paths, "LABEL_CACHE_FILE", path, raising=False)
    monkeypatch.setattr(Paths, "_setup", True, raising=False)
    monkeypatch.setattr(catalog, "_LABEL_CACHE", catalog._LabelStore())  # noqa: SLF001
    monkeypatch.setattr(catalog, "_ENTITY_CACHE", {})
    return path


def _provider(entities, **kwargs):
    return catalog.SportIndexFilterSearchProvider(_Client(entities, **kwargs))


def test_a_name_is_looked_up_once_and_then_remembered(cache_file) -> None:
    provider = _provider({"trnc:16": SimpleNamespace(name="Ligue 1", short_name=None)})

    assert provider.get_competition_option("trnc:16") == "Ligue 1"
    assert cache_file.exists()
    assert "Ligue 1" in json.loads(cache_file.read_text()).values()

    assert provider.get_competition_option("trnc:16") == "Ligue 1"
    assert provider._client.calls == 1  # noqa: SLF001


def test_the_name_survives_a_restart(cache_file) -> None:
    """ The point of the whole thing: a fresh process, no network. """
    _provider({"trnc:16": SimpleNamespace(name="Ligue 1", short_name=None)}).get_competition_option("trnc:16")

    catalog._LABEL_CACHE = catalog._LabelStore()  # noqa: SLF001
    catalog._ENTITY_CACHE.clear()  # noqa: SLF001
    offline = _Client({}, unreachable=True)

    label = catalog.SportIndexFilterSearchProvider(offline).get_competition_option("trnc:16")
    assert label == "Ligue 1"
    assert offline.calls == 0


def test_a_failed_lookup_is_never_remembered(cache_file) -> None:
    """ Otherwise "trnc:16" becomes the team's name, permanently. """
    provider = _provider({}, unreachable=True)

    assert provider.get_competition_option("trnc:16") == "trnc:16"

    stored = json.loads(cache_file.read_text()) if cache_file.exists() else {}
    assert "trnc:16" not in stored.values()


def test_an_unwritable_cache_does_not_break_a_lookup(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(Paths, "LABEL_CACHE_FILE", tmp_path / "nope" / "x.json", raising=False)
    monkeypatch.setattr(Paths, "_setup", True, raising=False)
    monkeypatch.setattr(catalog, "_LABEL_CACHE", catalog._LabelStore())  # noqa: SLF001
    monkeypatch.setattr(catalog, "_ENTITY_CACHE", {})
    monkeypatch.setattr(
        pathlib.Path, "mkdir", lambda *a, **k: (_ for _ in ()).throw(OSError("read-only")),
    )

    provider = _provider({"trnc:16": SimpleNamespace(name="Ligue 1", short_name=None)})
    assert provider.get_competition_option("trnc:16") == "Ligue 1"


def test_a_corrupt_cache_is_ignored_rather_than_fatal(cache_file) -> None:
    cache_file.write_text("{not json at all")

    provider = _provider({"trnc:16": SimpleNamespace(name="Ligue 1", short_name=None)})
    assert provider.get_competition_option("trnc:16") == "Ligue 1"

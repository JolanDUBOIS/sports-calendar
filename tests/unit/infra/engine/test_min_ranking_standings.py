""" "The top 5" must mean a table someone has played in.

A season publishes its table before it starts: every club on zero points, in no
meaningful order. It is not missing, so it passed every emptiness check — and
"top 5 of Ligue 1" in August resolved to Auxerre, Angers, Monaco, Troyes and
Lorient rather than PSG, Lens, Lille, Lyon and Marseille.
"""

from types import SimpleNamespace

import pytest
from sportindex.exceptions import ProviderNotFoundError

from sports_calendar.infra.engine.executors import MinRankingExecutor

LAST_SEASON = ["PSG", "Lens", "Lille", "Lyon", "Marseille"]
THIS_SEASON = ["Auxerre", "Angers", "Monaco", "Troyes", "Lorient"]


def _table(names: list[str], *, matches: int):
    entries = [
        SimpleNamespace(
            position=index + 1,
            matches=matches,
            competitor=SimpleNamespace(id=f"t-cpt:{name}", name=name),
        )
        for index, name in enumerate(names)
    ]
    return SimpleNamespace(kind="total", entries=entries)


def _season(table=None, *, raises: bool = False):
    class _Season:
        @property
        def standings(self):
            if raises:
                raise ProviderNotFoundError("nope")
            return [table] if table is not None else []

    return _Season()


def _competition(*seasons):
    return SimpleNamespace(seasons=list(seasons))


def _top_names(competition) -> list[str]:
    standings, reason = MinRankingExecutor._extract_total_standings(competition)
    assert standings is not None, f"no standings: {reason}"
    return [entry.competitor.name for entry in standings.entries]


def test_an_unplayed_season_falls_back_to_the_previous_one() -> None:
    """ The bug: August's empty table silently won. """
    competition = _competition(
        _season(_table(THIS_SEASON, matches=0)),
        _season(_table(LAST_SEASON, matches=34)),
    )
    assert _top_names(competition) == LAST_SEASON


def test_the_current_season_wins_as_soon_as_a_match_is_played() -> None:
    competition = _competition(
        _season(_table(THIS_SEASON, matches=1)),
        _season(_table(LAST_SEASON, matches=34)),
    )
    assert _top_names(competition) == THIS_SEASON


def test_a_played_season_is_used_without_looking_further_back() -> None:
    competition = _competition(_season(_table(THIS_SEASON, matches=38)))
    assert _top_names(competition) == THIS_SEASON


def test_a_missing_current_table_falls_back_too() -> None:
    competition = _competition(_season(None), _season(_table(LAST_SEASON, matches=34)))
    assert _top_names(competition) == LAST_SEASON


def test_a_raising_current_season_falls_back_too() -> None:
    competition = _competition(
        _season(raises=True), _season(_table(LAST_SEASON, matches=34))
    )
    assert _top_names(competition) == LAST_SEASON


def test_only_unplayed_seasons_yields_nothing_with_a_reason() -> None:
    """ Better an empty rule than five arbitrary clubs in the calendar. """
    competition = _competition(
        _season(_table(THIS_SEASON, matches=0)),
        _season(_table(LAST_SEASON, matches=0)),
    )
    standings, reason = MinRankingExecutor._extract_total_standings(competition)
    assert standings is None
    assert reason == "season not started"


def test_it_does_not_walk_back_further_than_last_season() -> None:
    """ Two seasons ago describes a squad that no longer exists. """
    competition = _competition(
        _season(_table(THIS_SEASON, matches=0)),
        _season(_table(THIS_SEASON, matches=0)),
        _season(_table(LAST_SEASON, matches=38)),
    )
    standings, _ = MinRankingExecutor._extract_total_standings(competition)
    assert standings is None


@pytest.mark.parametrize("competition", [None, SimpleNamespace(seasons=[])])
def test_nothing_to_read_is_reported_not_raised(competition) -> None:
    standings, reason = MinRankingExecutor._extract_total_standings(competition)
    assert standings is None
    assert reason in {"not found", "no seasons"}

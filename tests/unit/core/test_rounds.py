""" Deciding when a run of unnamed rounds is a group phase.

The numbers here are real: they come from what sport-index reports for these
competitions, so a change in the heuristic shows up as a named competition
being classified wrongly rather than as an abstract threshold moving.
"""

from types import SimpleNamespace

from sports_calendar.core.rounds import (
    GROUP_PHASE_LABEL,
    GROUP_PHASE_SLUG,
    LEAGUE_PHASE_LABEL,
    is_group_phase_event,
    selectable_rounds,
)


def _named(name: str, slug: str) -> SimpleNamespace:
    return SimpleNamespace(name=name, slug=slug)


def _matchdays(count: int) -> list[SimpleNamespace]:
    return [SimpleNamespace(name=None, slug=None) for _ in range(count)]


KNOCKOUTS = [
    _named("Round of 16", "round-of-16"),
    _named("Quarterfinals", "quarterfinals"),
    _named("Semifinals", "semifinals"),
    _named("Final", "final"),
]


def test_champions_league_offers_a_group_phase() -> None:
    """ 8 unnamed league-phase rounds, followed by knockouts. """
    rounds = [_named("Playoff round", "playoff-round"), *_matchdays(8), *KNOCKOUTS]
    offered = selectable_rounds(rounds)

    assert offered[GROUP_PHASE_SLUG] == GROUP_PHASE_LABEL


def test_group_phase_sits_where_its_rounds_actually_are() -> None:
    """ After qualifying, before the knockouts — not appended at the end. """
    rounds = [_named("Playoff round", "playoff-round"), *_matchdays(8), *KNOCKOUTS]
    order = list(selectable_rounds(rounds))

    assert order == ["playoff-round", GROUP_PHASE_SLUG, "round-of-16",
                     "quarterfinals", "semifinals", "final"]


def test_world_cup_group_stage_counts_despite_being_short() -> None:
    """ Only 3 group matchdays. """
    assert GROUP_PHASE_SLUG in selectable_rounds([*_matchdays(3), *KNOCKOUTS])


def test_ehf_champions_league_still_counts_at_fourteen() -> None:
    """ The longest real group phase found; a threshold of 12 would drop it. """
    assert GROUP_PHASE_SLUG in selectable_rounds([*_matchdays(14), *KNOCKOUTS])


def test_ligue_1_calls_it_a_league_phase() -> None:
    """ 34 matchdays plus a relegation play-off final — a season, not a group. """
    rounds = [*_matchdays(34), _named("Final", "final")]
    assert selectable_rounds(rounds)[GROUP_PHASE_SLUG] == LEAGUE_PHASE_LABEL


def test_euroleague_regular_season_is_a_league_phase() -> None:
    """ 38 rounds then play-offs — long enough to be plainly a season. """
    assert selectable_rounds([*_matchdays(38), *KNOCKOUTS])[GROUP_PHASE_SLUG] == LEAGUE_PHASE_LABEL


def test_six_nations_is_a_league_phase_not_a_group_phase() -> None:
    """ Only 5 rounds, but nothing follows them — a count alone mislabels this. """
    assert selectable_rounds(_matchdays(5)) == {GROUP_PHASE_SLUG: LEAGUE_PHASE_LABEL}


def test_premier_league_offers_its_season_as_one_entry() -> None:
    assert selectable_rounds(_matchdays(38)) == {GROUP_PHASE_SLUG: LEAGUE_PHASE_LABEL}


def test_matchdays_are_never_offered_individually() -> None:
    offered = selectable_rounds([*_matchdays(8), *KNOCKOUTS])
    assert list(offered).count(GROUP_PHASE_SLUG) == 1
    assert len(offered) == len(KNOCKOUTS) + 1


def test_group_phase_events_are_the_unnamed_ones() -> None:
    assert is_group_phase_event(SimpleNamespace(name=None, slug=None))
    assert not is_group_phase_event(SimpleNamespace(name="Final", slug="final"))
    assert not is_group_phase_event(None)

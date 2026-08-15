""" Copy that changes wording with the sport.

"Top Teams" is wrong for tennis. The old fix was to write every label as
"Top Teams" and run `.replace("Top Teams", "Top Players")` afterwards, which
worked only for the strings someone had remembered to spell that way — a label
worded any other way silently kept saying "teams" to a tennis user.

The templates replacing it fail differently and worse: a typo in a placeholder
name raises at render time, and an unfilled one reaches the screen as a literal
`{competitors}`. Both are invisible to the rest of the suite, so they are pinned
here.
"""

import pytest

from sports_calendar.core.selection import FilterType, Rule
from sports_calendar.interfaces.gui import copy

FOOTBALL = 1
TENNIS = 5
CYCLING = 65
MOTORSPORT = 11


def _every_string(sport_id: int) -> list[str]:
    return [
        text
        for text in (
            [copy.filter_type_label(f, sport_id) for f in FilterType]
            + [copy.filter_type_help(f, sport_id) for f in FilterType]
            + [copy.rule_label(r, sport_id) for r in Rule]
            + [copy.rule_help(r, sport_id) for r in Rule]
            + [copy.field_help(key, sport_id) for key in copy.FIELD_HELP]
        )
        if text
    ]


@pytest.mark.parametrize("sport_id", [FOOTBALL, TENNIS, CYCLING, MOTORSPORT, None])
def test_no_placeholder_ever_reaches_the_screen(sport_id) -> None:
    """ An unfilled `{competitors}` renders as those literal characters. """
    leaked = [text for text in _every_string(sport_id) if "{" in text or "}" in text]
    assert not leaked, leaked


@pytest.mark.parametrize("sport_id", [TENNIS, CYCLING])
def test_individual_sports_are_never_told_about_teams(sport_id) -> None:
    """ The whole point. Tennis has players. """
    said_teams = [text for text in _every_string(sport_id) if "team" in text.lower()]
    assert not said_teams, said_teams


def test_team_sports_are_never_told_about_players(sport_id=FOOTBALL) -> None:
    # "player" appears legitimately nowhere in the team wording; if it starts to,
    # it is the same bug in the other direction.
    said_players = [text for text in _every_string(sport_id) if "player" in text.lower()]
    assert not said_players, said_players


def test_the_labels_still_read_as_before() -> None:
    assert copy.filter_type_label(FilterType.MIN_RANKING, FOOTBALL) == "Top Teams"
    assert copy.filter_type_label(FilterType.MIN_RANKING, TENNIS) == "Top Players"
    assert copy.filter_type_label(FilterType.WORLD_RANKING, TENNIS) == "Top Players (world rankings)"


def test_an_unmapped_sport_falls_back_to_teams() -> None:
    """ `sport_id=None` happens wherever a presenter has no parent item. """
    assert copy.filter_type_label(FilterType.MIN_RANKING, None) == "Top Teams"


@pytest.mark.parametrize("sport_id", [FOOTBALL, TENNIS])
def test_a_help_text_naming_another_menu_entry_names_the_real_one(sport_id) -> None:
    """ The reason cross-references are built rather than typed.

    The world-ranking help used to tell people to pick "Top of the world
    rankings", which the menu had not said for some time. Anyone who followed
    that instruction went looking for an entry that did not exist.
    """
    world = copy.filter_type_label(FilterType.WORLD_RANKING, sport_id)
    minimum = copy.filter_type_label(FilterType.MIN_RANKING, sport_id)

    assert world in copy.filter_type_help(FilterType.MIN_RANKING, sport_id)
    assert minimum in copy.filter_type_help(FilterType.WORLD_RANKING, sport_id)

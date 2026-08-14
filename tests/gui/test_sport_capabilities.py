""" The UI only offers what the backend can actually deliver.

Two silent failures are guarded here:

- adding a sport with no event class builds a calendar that raises at sync time;
- picking a filter the sport cannot satisfy yields an empty calendar with no
  error at all, which is worse.
"""

from nicegui import ui
from nicegui.testing import User

from sports_calendar.application.selection import SelectionService
from sports_calendar.core.selection import FilterType
from sports_calendar.interfaces.gui import copy

SELECTION = "caps"


def _select_options(user: User, *, containing: str) -> dict:
    """ The options of the one select that offers `containing` as a key. """
    for element in user.find(ui.select).elements:
        if containing in (element.options or {}):
            return element.options
    raise AssertionError(f"no select offering {containing!r} was rendered")


async def test_add_sport_offers_only_supported_sports(user: User) -> None:
    SelectionService.add_empty_selection(SELECTION)
    await user.open(f"/selections/{SELECTION}")

    user.find(marker="add-card").click()
    await user.should_see("Add a sport")

    options = next(iter(user.find(ui.select).elements)).options
    assert set(options) == {1, 5, 11}, "the sport list must match EVENT_TYPE_MAP"
    assert 4 not in options, "ice-hockey has no event class and must not be offered"


async def _open_filter_modal(user: User, sport_id: int) -> None:
    SelectionService.add_empty_selection(SELECTION)
    SelectionService.add_empty_item(SELECTION, sport_id, name="s")
    await user.open(f"/selections/{SELECTION}")

    next(iter(user.find(ui.expansion).elements)).value = True
    user.find(copy.ADD_RULE_BUTTON).click()
    await user.should_see(copy.FOLLOW_TITLE)


async def test_football_is_not_offered_race_sessions(user: User) -> None:
    """ SessionsExecutor only understands staged events. """
    await _open_filter_modal(user, sport_id=1)

    options = _select_options(user, containing=FilterType.COMPETITIONS.value)
    assert FilterType.SESSIONS.value not in options
    assert FilterType.MIN_RANKING.value in options


async def test_motorsport_is_offered_sessions_but_not_rankings(user: User) -> None:
    """ Motorsport has race weekends but no league table to rank. """
    await _open_filter_modal(user, sport_id=11)

    options = _select_options(user, containing=FilterType.COMPETITIONS.value)
    assert FilterType.SESSIONS.value in options
    assert FilterType.MIN_RANKING.value not in options


async def test_tennis_gets_world_rankings_instead_of_a_league_table(user: User) -> None:
    """ Tennis has no standings, but ATP/WTA are exactly what it does have. """
    await _open_filter_modal(user, sport_id=5)

    options = _select_options(user, containing=FilterType.COMPETITIONS.value)
    assert FilterType.SESSIONS.value not in options
    assert FilterType.MIN_RANKING.value not in options
    assert FilterType.WORLD_RANKING.value in options
    assert FilterType.COMPETITORS.value in options


async def test_motorsport_is_not_offered_world_rankings(user: User) -> None:
    """ No governing body publishes a motorsport standing order. """
    await _open_filter_modal(user, sport_id=11)

    options = _select_options(user, containing=FilterType.COMPETITIONS.value)
    assert FilterType.WORLD_RANKING.value not in options


async def test_choosing_world_ranking_offers_the_atp_and_wta_tables(user: User) -> None:
    """ Selecting the type must rebuild the form with that sport's rankings. """
    await _open_filter_modal(user, sport_id=5)

    type_select = next(
        element for element in user.find(ui.select).elements
        if FilterType.COMPETITIONS.value in (element.options or {})
    )
    type_select.set_value(FilterType.WORLD_RANKING.value)
    await user.should_see("Ranking")

    assert _select_options(user, containing=5) == {5: "ATP (men)", 6: "WTA (women)"}

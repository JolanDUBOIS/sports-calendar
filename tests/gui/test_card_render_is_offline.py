""" Rendering a card must not wait on the network.

A rule's subtitle names what it holds — "Ligue 1, LaLiga +3" — and resolving
those ids costs about a second each. Building them while rendering blocked the
selection page for twenty seconds on a real selection, with nothing on screen.
"""

import threading

import pytest
from nicegui.testing import User

from sports_calendar.application.selection import SelectionService
from sports_calendar.core.selection import CompetitionsFilterFields, SelectionFilter

SELECTION = "my-sel"
FOOTBALL = 1


@pytest.fixture
def blocking_lookups(monkeypatch):
    """ Make every id-to-name lookup hang until released. """
    released = threading.Event()

    def slow_get_options(self, entity_ids, entity_cls=None):  # noqa: ARG001
        assert released.wait(timeout=5), "lookup was never released"
        return {entity_id: f"Name {entity_id}" for entity_id in entity_ids}

    from sports_calendar.interfaces.gui.catalog import SportIndexFilterSearchProvider

    monkeypatch.setattr(SportIndexFilterSearchProvider, "get_competition_options", slow_get_options)
    monkeypatch.setattr(SportIndexFilterSearchProvider, "get_competitor_options", slow_get_options)
    return released


async def _open_page(user: User) -> None:
    SelectionService.add_empty_selection(SELECTION)
    item = SelectionService.add_empty_item(SELECTION, FOOTBALL, name="Football")
    SelectionService.add_filter(
        item.uid,
        SelectionFilter(
            sport_id=FOOTBALL,
            name="UCL",
            fields=CompetitionsFilterFields(competition_ids=["trnc:7"]),
        ),
    )
    await user.open(f"/selections/{SELECTION}")


async def test_the_page_renders_before_the_lookups_answer(user: User, blocking_lookups) -> None:
    await _open_page(user)

    # The card is on screen with its title, while the name lookup is still hanging.
    await user.should_see("UCL")
    assert not blocking_lookups.is_set()


async def test_the_subtitle_appears_once_the_lookup_returns(user: User, blocking_lookups) -> None:
    await _open_page(user)
    await user.should_see("UCL")

    blocking_lookups.set()
    await user.should_see("Name trnc:7")

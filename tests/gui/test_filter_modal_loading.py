""" The Edit dialog opens before it knows what to put in it.

Its defaults come off the network — the names of the competitions a rule holds,
the rounds they share — which is roughly a second per competition. Building them
first meant the button sat there doing nothing for as long as fifteen seconds,
and clicking again (the natural response) stacked a second dialog on the first.
"""

import threading

import pytest
from nicegui.testing import User

from sports_calendar.application.selection import SelectionService
from sports_calendar.core.selection import CompetitionsFilterFields, SelectionFilter

SELECTION = "my-sel"
FOOTBALL = 1


class _BlockingProvider:
    """ A search provider that will not answer until released.

    Stands in for the real one's latency without sleeping, so the test asserts
    on ordering rather than on timing.
    """

    def __init__(self):
        self.released = threading.Event()
        self.calls = 0

    def _wait(self):
        self.calls += 1
        assert self.released.wait(timeout=5), "provider was never released"

    def get_competition_options(self, competition_ids):
        self._wait()
        return {competition_id: f"Competition {competition_id}" for competition_id in competition_ids}

    def get_shared_rounds(self, competition_ids):  # noqa: ARG002
        self._wait()
        return {"final": "Final"}

    def search_competition(self, query, sport_id):  # noqa: ARG002
        return {}

    def search_competitor(self, query, sport_id):  # noqa: ARG002
        return {}


@pytest.fixture
def blocking_provider(monkeypatch):
    from sports_calendar.interfaces.gui.views import item_card

    provider = _BlockingProvider()
    monkeypatch.setattr(item_card, "SportIndexFilterSearchProvider", lambda client: provider)
    return provider


async def _open_page_with_a_competitions_rule(user: User) -> None:
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


async def test_the_dialog_is_on_screen_while_its_fields_are_still_loading(
    user: User, click_one, blocking_provider
) -> None:
    await _open_page_with_a_competitions_rule(user)

    click_one(user, "Edit")

    # The dialog and its placeholder are up even though the provider has not
    # answered yet — this is the whole point of the change.
    await user.should_see("Looking up what this rule follows")
    assert not blocking_provider.released.is_set()

    blocking_provider.released.set()
    await user.should_see("Competitions")
    await user.should_not_see("Looking up what this rule follows")


async def test_save_is_disabled_until_the_fields_arrive(
    user: User, click_one, blocking_provider
) -> None:
    """ Otherwise Save on a half-built form would overwrite the rule with nothing. """
    await _open_page_with_a_competitions_rule(user)

    click_one(user, "Edit")
    await user.should_see("Looking up what this rule follows")

    save = next(iter(user.find(marker="modal-confirm").elements))
    assert save.enabled is False

    blocking_provider.released.set()
    await user.should_see("Competitions")
    assert save.enabled is True


async def test_the_edit_button_ignores_clicks_while_it_is_working(
    user: User, click_one, blocking_provider
) -> None:
    """ The double-click that used to stack dialogs. """
    await _open_page_with_a_competitions_rule(user)

    click_one(user, "Edit")
    await user.should_see("Looking up what this rule follows")

    edit = next(iter(user.find("Edit").elements))
    assert edit.enabled is False, "a second click would open a second dialog"

    blocking_provider.released.set()
    await user.should_see("Competitions")
    assert edit.enabled is True

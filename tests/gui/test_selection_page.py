""" Item-level flows on the single-selection page.

The expansion-state assertions are the regressions that matter: creating or
deleting an item used to call `ui.navigate.reload()`, which rebuilt the page and
collapsed every open card.
"""

from nicegui import ui
from nicegui.testing import User

from sports_calendar.application.selection import SelectionService

SELECTION = "my-sel"
FOOTBALL = 1
TENNIS = 5


async def _open_with_items(user: User, *item_names: str) -> None:
    SelectionService.add_empty_selection(SELECTION)
    for name in item_names:
        SelectionService.add_empty_item(SELECTION, 1, name=name)
    await user.open(f"/selections/{SELECTION}")


async def test_empty_selection_shows_placeholder(user: User) -> None:
    await _open_with_items(user)
    await user.should_see("No sports yet")


async def test_selection_page_lists_its_items(user: User) -> None:
    await _open_with_items(user, "first", "second")
    assert len(user.find(ui.expansion).elements) == 2
    await user.should_not_see("No sports yet")


async def test_adding_an_item_keeps_other_cards_expanded(user: User) -> None:
    """ Regression: the page used to reload, collapsing every open card. """
    await _open_with_items(user, "first")

    existing = next(iter(user.find(ui.expansion).elements))
    existing.value = True  # the user opens the card

    user.find(marker="add-card").click()
    await user.should_see("Add a sport")
    # A different sport: football is already on the page, and a sport already in
    # the calendar is no longer offered.
    next(iter(user.find(ui.select).elements)).set_value(TENNIS)
    user.find(marker="modal-confirm").click()

    await user.should_see("Sport added.")
    assert existing.value is True, "existing card collapsed: the page was reloaded"
    assert len(user.find(ui.expansion).elements) == 2


async def test_adding_an_item_clears_the_empty_placeholder(user: User) -> None:
    await _open_with_items(user)
    await user.should_see("No sports yet")

    user.find(marker="add-card").click()
    await user.should_see("Add a sport")
    next(iter(user.find(ui.select).elements)).set_value(FOOTBALL)
    user.find(marker="modal-confirm").click()

    await user.should_see("Sport added.")
    await user.should_not_see("No sports yet")


async def test_a_sport_already_in_the_calendar_is_not_offered_again(user: User) -> None:
    """ An item is only a grouping by sport, and its filters union with every
    other item's — so a second football card would behave identically to putting
    those rules on the first one. """
    await _open_with_items(user, "first")

    user.find(marker="add-card").click()
    await user.should_see("Add a sport")

    options = next(iter(user.find(ui.select).elements)).options
    assert FOOTBALL not in options
    assert TENNIS in options


async def test_the_menu_explains_itself_when_every_sport_is_taken(user: User) -> None:
    SelectionService.add_empty_selection(SELECTION)
    for sport_id in (FOOTBALL, TENNIS, 11):  # every supported sport in the stub
        SelectionService.add_empty_item(SELECTION, sport_id)
    await user.open(f"/selections/{SELECTION}")

    user.find(marker="add-card").click()
    await user.should_see("Every sport is already in this calendar")


async def test_deleting_an_item_removes_only_that_card(user: User, click_one) -> None:
    await _open_with_items(user, "first", "second")

    survivor = list(user.find(ui.expansion).elements)[1]
    survivor.value = True

    # Each card has its own Delete button; click the first card's.
    click_one(user, marker="card-delete", index=0)
    await user.should_see("also removes")
    click_one(user, marker="modal-confirm")

    await user.should_see("Sport removed.")
    assert len(user.find(ui.expansion).elements) == 1
    assert survivor.value is True, "surviving card collapsed: the page was reloaded"


async def test_deleting_the_last_item_restores_the_placeholder(user: User, click_one) -> None:
    await _open_with_items(user, "only")

    click_one(user, marker="card-delete")
    await user.should_see("also removes")
    click_one(user, marker="modal-confirm")

    await user.should_see("Sport removed.")
    await user.should_see("No sports yet")

from nicegui.testing import User

from sports_calendar.application.selection import SelectionService


async def test_selections_page_renders_when_empty(user: User) -> None:
    await user.open("/selections")
    await user.should_see("My calendars")


async def test_create_selection_persists_it(user: User) -> None:
    """ The create-selection flow, end to end through the real modal. """
    await user.open("/selections")

    user.find(marker="add-card").click()
    await user.should_see("New calendar")

    user.find("Name").type("my-teams")
    user.find("Create").click()

    await user.should_see("created")
    assert SelectionService.selection_exists("my-teams")


async def test_created_selection_appears_in_place(user: User, click_one) -> None:
    """ Regression: creating a selection used to reload the whole page. """
    SelectionService.add_empty_selection("existing")
    await user.open("/selections")
    assert len(user.find(marker="selection-card").elements) == 1

    click_one(user, marker="add-card")
    await user.should_see("New calendar")
    user.find("Name").type("brand-new")
    click_one(user, marker="modal-confirm")

    await user.should_see("created")
    assert len(user.find(marker="selection-card").elements) == 2
    await user.should_see("existing")


async def test_deleting_a_selection_removes_its_card(user: User, click_one) -> None:
    SelectionService.add_empty_selection("doomed")
    await user.open("/selections")
    await user.should_see("doomed")

    click_one(user, "Delete")
    await user.should_see("goes with it")
    click_one(user, marker="modal-confirm")

    await user.should_see("deleted")
    # Not `should_not_see("doomed")`: the confirmation toast names the selection.
    await user.should_not_see(marker="selection-card")
    assert not SelectionService.selection_exists("doomed")

from nicegui import ui
from nicegui.testing import User

from sports_calendar.application.selection import SelectionService


async def test_selections_page_renders_when_empty(user: User) -> None:
    await user.open("/selections")
    await user.should_see("All Selections")


async def test_create_selection_persists_it(user: User) -> None:
    """ The create-selection flow, end to end through the real modal. """
    await user.open("/selections")

    user.find(ui.card).click()  # on an empty list the only card is the AddCard
    await user.should_see("Create Selection")

    user.find("Selection name").type("my-teams")
    user.find("Create").click()

    await user.should_see("created")
    assert SelectionService.selection_exists("my-teams")

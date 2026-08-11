""" Fixtures for the headless GUI tests.

These drive the real NiceGUI pages in-process via NiceGUI's user simulation, so
they exercise the actual click paths without a browser.

`user` is defined here rather than taken from `nicegui.testing.user_plugin`
because it must run *after* the isolation fixture. NiceGUI's own `user` is an
async fixture, and pytest-asyncio sets those up before sync autouse fixtures —
so relying on autouse ordering silently booted the app against the real config.
Declaring the dependency explicitly makes the order guaranteed.
"""

from pathlib import Path
from types import SimpleNamespace

import pytest
from nicegui import events
from nicegui.testing import User
from nicegui.testing.user_simulation import user_simulation

GUI_MAIN_FILE = Path(__file__).parents[2] / "src/sports_calendar/interfaces/gui/app.py"


@pytest.fixture
def isolated_selections(tmp_path, monkeypatch):
    """ Redirect selection storage to a temp dir.

    Non-negotiable: without it a GUI test that creates or deletes a selection
    would write to the developer's real config/selections folder.
    """
    # Imported here, not at module level: other tests reload sports_calendar
    # modules, so a module-level reference can go stale and we would patch a
    # class object the app under test no longer uses.
    import sports_calendar.infra.setup as setup_module
    from sports_calendar.application.selection.registry import SelectionRegistry
    from sports_calendar.infra.config import Paths

    selections_dir = tmp_path / "selections"
    selections_dir.mkdir(parents=True)

    # These are annotation-only on Paths until setup() runs, hence raising=False.
    monkeypatch.setattr(Paths, "CONFIG_DIR", tmp_path, raising=False)
    monkeypatch.setattr(Paths, "SELECTIONS_FOLDER", selections_dir, raising=False)
    monkeypatch.setattr(Paths, "LOG_DIR", tmp_path / "logs", raising=False)
    monkeypatch.setattr(Paths, "_setup", True, raising=False)

    # Stop app.py's __main__ block from re-pointing Paths at the real config dir.
    # Patched on the module object, not by dotted string: runpy rewrites
    # sys.modules entries and breaks monkeypatch's string resolution.
    monkeypatch.setattr(setup_module, "init_environment", lambda: None)

    # The registry is a process-wide singleton that refuses re-initialization,
    # and app.py's run() initializes it on every simulated boot.
    monkeypatch.setattr(SelectionRegistry, "_initialized", False)
    monkeypatch.setattr(SelectionRegistry, "_selections", [])

    return selections_dir


FAKE_SPORTS = [
    SimpleNamespace(id="spt:1", name="football"),
    SimpleNamespace(id="spt:11", name="motorsport"),
]


@pytest.fixture
def offline_sport_index(monkeypatch):
    """ Stub the sport-index calls the GUI makes while rendering.

    Rendering an item calls `get_sport_name`, and the create-item modal calls
    `client.list(Sport)`; both hit the network. Stubbing them keeps GUI tests
    hermetic and fast. `Sport.decode_id` is a classmethod, so the fake sports
    only need a prefixed `id` and a `name`.
    """
    from sportindex import SportClient

    from sports_calendar.interfaces.gui.presenters import selection, selection_item

    monkeypatch.setattr(SportClient, "list", lambda self, entity_cls, **kwargs: FAKE_SPORTS)

    # Patched where they are used: both presenter modules bound the name at
    # import time via `from ..catalog import get_sport_name`.
    for module in (selection, selection_item):
        monkeypatch.setattr(module, "get_sport_name", lambda client, sport_id: f"Sport-{sport_id}")

    return FAKE_SPORTS


@pytest.fixture
async def user(isolated_selections, offline_sport_index):  # noqa: ARG001 - ordering dependency
    """ A simulated user driving the real GUI pages against isolated storage. """
    async with user_simulation(main_file=GUI_MAIN_FILE) as simulated_user:
        yield simulated_user


def _click_one(
    user: User,
    target: str | type | None = None,
    *,
    marker: str | None = None,
    index: int = 0,
) -> None:
    """ Click exactly one matched element.

    Two reasons this exists instead of `user.find(...).click()`:

    1. `find()` returns every match and `click()` fires all of them, which is
       wrong when several cards each carry a "Delete" button.
    2. `click()` iterates the element's live listener dict. Our handlers remove
       cards in place, which mutates that dict mid-iteration and raises
       "dictionary changed size during iteration". Iterating a copy avoids it.
       This is a limitation of the simulator only: a real browser dispatches the
       click over the websocket, outside that loop.
    """
    interaction = user.find(marker=marker) if marker is not None else user.find(target)
    element = list(interaction.elements)[index]

    for listener in list(element._event_listeners.values()):  # noqa: SLF001
        if listener.element_id != element.id:
            continue
        events.handle_event(
            listener.handler,
            events.GenericEventArguments(sender=element, client=user.client, args=None),
        )


@pytest.fixture
def click_one():
    """ Expose `_click_one` to tests without import-path gymnastics. """
    return _click_one

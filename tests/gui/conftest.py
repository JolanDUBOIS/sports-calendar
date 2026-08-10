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

import pytest
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


@pytest.fixture
async def user(isolated_selections):  # noqa: ARG001 - ordering dependency
    """ A simulated user driving the real GUI pages against isolated storage. """
    async with user_simulation(main_file=GUI_MAIN_FILE) as simulated_user:
        yield simulated_user

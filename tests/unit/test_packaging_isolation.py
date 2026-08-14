""" Verifies the backend/UI split holds at runtime.

`make lint-imports` checks the split statically, but static analysis cannot prove
that a module tree is *importable* without an optional extra — a single
transitive import is enough to break an install. These tests simulate a partial
install by blocking the other extra's packages at import time.
"""

import importlib
import sys
from contextlib import contextmanager

import pytest

# Packages installed by the `backend` extra, plus the google namespace packages
# its libraries live under.
BACKEND_PACKAGES = [
    "googleapiclient",
    "google_auth_oauthlib",
    "google_auth_httplib2",
    "google.auth",
    "google.oauth2",
]

# Packages installed by the `ui` extra.
UI_PACKAGES = ["nicegui"]


class _BlockedImportFinder:
    """ Meta path finder that makes the given packages look uninstalled. """

    def __init__(self, blocked: list[str]):
        self._blocked = tuple(blocked)

    def _is_blocked(self, fullname: str) -> bool:
        return any(
            fullname == name or fullname.startswith(name + ".")
            for name in self._blocked
        )

    def find_spec(self, fullname, path=None, target=None):
        if self._is_blocked(fullname):
            raise ImportError(
                f"{fullname!r} is blocked to simulate a partial install",
                name=fullname,
            )
        # Falling off the end hands the import back to the real finders.


def _purge(blocked: list[str]) -> None:
    """ Drop our package and the blocked ones so imports are re-evaluated. """
    for name in list(sys.modules):
        if name.startswith("sports_calendar") or any(
            name == b or name.startswith(b + ".") for b in blocked
        ):
            del sys.modules[name]


@contextmanager
def partial_install(blocked: list[str]):
    """ Run the block with `blocked` packages appearing to be uninstalled.

    The original modules are snapshotted and restored afterwards. Without that,
    re-importing under the block replaces every `sports_calendar` class object,
    and any later test holding a reference to one (a fixture patching a
    singleton, say) would silently be patching a class the app no longer uses.
    """
    finder = _BlockedImportFinder(blocked)
    saved = {
        name: module for name, module in sys.modules.items()
        if name.startswith("sports_calendar") or any(
            name == b or name.startswith(b + ".") for b in blocked
        )
    }
    _purge(blocked)
    sys.meta_path.insert(0, finder)
    try:
        yield
    finally:
        sys.meta_path.remove(finder)
        _purge(blocked)
        sys.modules.update(saved)


# --- the blocker itself must work, or every test below is a false green ---

def test_blocker_actually_blocks():
    with partial_install(BACKEND_PACKAGES), pytest.raises(ImportError):
        importlib.import_module("sports_calendar.infra.google_calendar")


# --- UI-only install: no `backend` extra ---

@pytest.mark.parametrize("module", [
    "sports_calendar.core",
    "sports_calendar.core.calendar",
    "sports_calendar.infra.engine",
    "sports_calendar.infra.storage",
    "sports_calendar.application.selection",
    "sports_calendar.application.workflows",
    "sports_calendar.application.workflows.build_calendar",
    "sports_calendar.interfaces.cli",
    "sports_calendar.interfaces.gui.app",
])
def test_ui_only_install_can_import(module):
    """ A UI install must not need any Google Calendar library. """
    with partial_install(BACKEND_PACKAGES):
        importlib.import_module(module)


# --- backend-only install: no `ui` extra ---

@pytest.mark.parametrize("module", [
    "sports_calendar.core",
    "sports_calendar.core.calendar",
    "sports_calendar.infra.engine",
    "sports_calendar.infra.google_calendar",
    "sports_calendar.application.workflows",
    "sports_calendar.application.workflows.build_calendar",
    "sports_calendar.application.workflows.run_selection",
    "sports_calendar.application.workflows.clear_calendar",
    "sports_calendar.interfaces.cli",
    "sports_calendar.__main__",
])
def test_backend_only_install_can_import(module):
    """ A backend install must not need NiceGUI. """
    with partial_install(UI_PACKAGES):
        importlib.import_module(module)

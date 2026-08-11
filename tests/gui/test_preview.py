""" The event preview drawer.

`resolve_events` is stubbed: the real one scrapes sport-index over the network,
which has no place in a unit-speed test. What is under test is the drawer's
behaviour — ordering, grouping, empty and error states.
"""

from datetime import datetime
from types import SimpleNamespace

from nicegui.testing import User

import sports_calendar.interfaces.gui.views.preview as preview_module
from sports_calendar.application.selection import SelectionService
from sports_calendar.core.calendar import SportsEventCollection

SELECTION = "preview-sel"


def _event(summary: str, start: datetime) -> SimpleNamespace:
    return SimpleNamespace(summary=summary, start=start, end=start)


async def _open(user: User) -> None:
    SelectionService.add_empty_selection(SELECTION)
    await user.open(f"/selections/{SELECTION}")


async def test_preview_is_not_built_until_asked(user: User, monkeypatch) -> None:
    """ Resolving hits the network, so it must never happen on page load. """
    calls = []
    monkeypatch.setattr(preview_module, "resolve_events", lambda name: calls.append(name))

    await _open(user)

    await user.should_see("Nothing loaded yet")
    assert calls == []


async def test_preview_lists_events_in_date_order(user: User, monkeypatch, click_one) -> None:
    events = SportsEventCollection([
        _event("Later match", datetime(2026, 9, 2, 20, 0)),
        _event("Earlier match", datetime(2026, 9, 1, 18, 0)),
    ])
    monkeypatch.setattr(preview_module, "resolve_events", lambda name: events)

    await _open(user)
    click_one(user, "Build preview")

    await user.should_see("2 events")
    await user.should_see("Earlier match")
    await user.should_see("Later match")
    await user.should_see("Tue 01 Sep 2026")


async def test_preview_reports_an_empty_selection(user: User, monkeypatch, click_one) -> None:
    monkeypatch.setattr(preview_module, "resolve_events", lambda name: SportsEventCollection([]))

    await _open(user)
    click_one(user, "Build preview")

    await user.should_see("produced no events")


async def test_preview_surfaces_a_failure_instead_of_crashing(user: User, monkeypatch, click_one) -> None:
    def _fail(name):
        raise RuntimeError("sport-index is unreachable")

    monkeypatch.setattr(preview_module, "resolve_events", _fail)

    await _open(user)
    click_one(user, "Build preview")

    await user.should_see("Could not build the preview.")
    await user.should_see("sport-index is unreachable")

""" The event preview drawer.

Answers the question a selection screen otherwise leaves open: "what does this
actually put in my calendar?". It runs the same resolver the backend uses, so
what you see here is what a sync would produce.

Resolving hits the network, so it never runs automatically — the user asks for
it, and it runs off the event loop so the page stays responsive.
"""

import logging
from datetime import datetime

from nicegui import run, ui

from sports_calendar.application.workflows import resolve_events
from sports_calendar.core.calendar import SportsEventCollection

from ..components import explanation
from ..copy import PREVIEW_EMPTY, PREVIEW_INTRO, PREVIEW_STALE

logger = logging.getLogger(__name__)


def _sort_key(event) -> tuple[int, float]:
    """ Order events in time, tolerating missing or mixed-awareness datetimes.

    Events that carry no usable start are pushed to the end rather than blowing
    up the whole preview.
    """
    start = getattr(event, "start", None)
    if not isinstance(start, datetime):
        return (1, 0.0)
    # Naive and aware datetimes cannot be compared; normalise to a timestamp.
    reference = start.replace(tzinfo=None)
    return (0, reference.timestamp())


def _render_events(container: ui.column, events: SportsEventCollection) -> None:
    container.clear()
    ordered = sorted(events, key=_sort_key)

    with container:
        if not ordered:
            explanation(PREVIEW_EMPTY)
            return

        ui.label(f"{len(ordered)} events").classes("text-xs uppercase text-gray-500")

        current_day = None
        for event in ordered:
            start = event.start if isinstance(getattr(event, "start", None), datetime) else None
            day = start.date() if start else None

            if day != current_day:
                current_day = day
                heading = day.strftime("%a %d %b %Y") if day else "Date unknown"
                ui.label(heading).classes(
                    "text-xs font-bold uppercase text-gray-500 mt-3"
                )

            with ui.card().classes("w-full p-2 shadow-none border border-gray-200"):
                ui.label(event.summary).classes("text-sm font-medium leading-tight")
                if start:
                    ui.label(start.strftime("%H:%M")).classes("text-xs text-gray-500")


def render_preview_drawer(selection_name: str) -> ui.right_drawer:
    """ Build the (initially hidden) preview drawer for a selection. """
    drawer = ui.right_drawer(value=False, fixed=True).classes("bg-gray-50").props("width=380 bordered")

    with drawer, ui.column().classes("w-full gap-2 p-2"):
        ui.label("Event preview").classes("text-lg font-bold")
        explanation(PREVIEW_INTRO)

        results = ui.column().classes("w-full gap-1 mt-2")
        with results:
            explanation(PREVIEW_STALE)

        async def load() -> None:
            results.clear()
            with results:
                ui.spinner(size="lg").classes("self-center mt-4")
                explanation("Resolving events — this can take a moment.")

            try:
                events = await run.io_bound(resolve_events, selection_name)
            except Exception as exc:  # noqa: BLE001 - surfaced to the user below
                logger.exception("Preview failed for selection '%s'", selection_name)
                results.clear()
                with results:
                    ui.label("Could not build the preview.").classes(
                        "text-sm font-medium text-red-600"
                    )
                    ui.label(str(exc)).classes("text-xs text-red-500 break-words")
                return

            _render_events(results, events)

        ui.button("Build preview", on_click=load).props("outline").classes("w-full")

    return drawer

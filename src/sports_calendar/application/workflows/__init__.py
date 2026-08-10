""" Application workflows.

Only shared workflows are re-exported here, so that importing this package stays
safe for a UI-only install (no `backend` extra, no Google Calendar libraries).

Backend-only workflows live in `run_selection` and `clear_calendar` and must be
imported from their module path directly:

    from sports_calendar.application.workflows.run_selection import run_selection
    from sports_calendar.application.workflows.clear_calendar import clear_calendar
"""

from .build_calendar import build_calendar

__all__ = ["build_calendar"]

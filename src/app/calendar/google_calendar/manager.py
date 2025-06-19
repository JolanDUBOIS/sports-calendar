from __future__ import annotations
import traceback
from datetime import datetime

from icalendar import Calendar

from . import logger
from .auth import GoogleAuthManager
from .api_client import GoogleCalendarAPI


class GoogleCalendarManager:
    """ TODO """

    def __init__(self, google_calendar_api: GoogleCalendarAPI):
        """ TODO """
        self.api = google_calendar_api

    def add_calendar(
        self,
        calendar: Calendar,
        **kwargs
    ) -> None:
        """ Add a calendar to Google Calendar """
        self.api.add_events(events=calendar.events, **kwargs)

    def clear_calendar(self, scope: str | None = None, date_from: str | None = None, date_to: str | None = None, verbose: bool = False) -> None:
        """ Clear events from the Google Calendar based on the specified scope """
        today = datetime.now().date().isoformat()
        if scope is None:
            self.api.delete_events(date_from=date_from, date_to=date_to, verbose=verbose)
        elif scope == 'all':
            self.api.delete_events(verbose=verbose)
        elif scope == 'future':
            self.api.delete_events(date_from=today, verbose=verbose)
        elif scope == 'past':
            self.api.delete_events(date_to=today, verbose=verbose)
        else:
            logger.error(f"Invalid scope '{scope}' specified. Valid options are 'all', 'future', or 'past'.")
            raise ValueError(f"Invalid scope '{scope}' specified. Valid options are 'all', 'future', or 'past'.")

    @classmethod
    def from_defaults(cls, gcal_id: str) -> GoogleCalendarManager:
        """ Create a GoogleCalendarManager with default settings """
        auth = GoogleAuthManager()
        api = GoogleCalendarAPI(auth_manager=auth, calendar_id=gcal_id)
        return cls(api)

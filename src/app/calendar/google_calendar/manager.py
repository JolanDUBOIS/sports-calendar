from __future__ import annotations
import traceback

from icalendar import Calendar

from . import logger
from .auth import GoogleAuthManager
from .api_client import GoogleCalendarAPI


class GoogleCalendarManager:
    """ TODO """

    def __init__(self, google_calendar_api: GoogleCalendarAPI):
        """ TODO """
        self.api = google_calendar_api

    def add_calendar(self, calendar: Calendar, date_from: str | None = None, date_to: str | None = None) -> None:
        """ Add a calendar to Google Calendar """
        for event in calendar.events:
            try:
                if date_from and event.get('dtstart').dt < date_from:
                    continue
                if date_to and event.get('dtend').dt > date_to:
                    continue
                self.api.add_event(event, raise_on_error=True)
            except Exception as e:
                logger.error(f"Error adding event {event.get('summary')}: {e}")
                logger.error(traceback.format_exc())
                raise
        logger.info(f"Added {len(calendar.events)} events to Google Calendar.")

    @classmethod
    def from_defaults(cls) -> GoogleCalendarManager:
        """ Create a GoogleCalendarManager with default settings """
        auth = GoogleAuthManager()
        api = GoogleCalendarAPI(auth)
        return cls(api)

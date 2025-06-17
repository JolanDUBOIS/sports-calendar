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
        date_from: str | None = None,
        date_to: str | None = None,
        verbose: bool = False
    ) -> None:
        """ Add a calendar to Google Calendar """
        N_events = len(calendar.events)
        for i, event in enumerate(calendar.events):
            if verbose:
                print(f"\r{' ' * 80}\rAdding event {i + 1}/{N_events}: {event.get('summary')}", end='\r')
            try:
                if date_from and event.get('dtstart').dt < date_from:
                    continue
                if date_to and event.get('dtend').dt > date_to:
                    continue
                self.api.add_event(event)
            except Exception as e:
                logger.error(f"Error adding event {event.get('summary')}: {e}")
                logger.debug("Traceback:\n%s", traceback.format_exc())
                raise
        logger.info(f"Added {len(calendar.events)} events to Google Calendar.")

    def clear_calendar(self, scope: str = 'future') -> None:
        """ Clear events from the Google Calendar based on the specified scope """
        today = datetime.now().isoformat()
        if scope == 'all':
            self.api.delete_events()
        elif scope == 'future':
            self.api.delete_events(date_from=today)
        elif scope == 'past':
            self.api.delete_events(date_to=today)
        else:
            logger.error(f"Invalid scope '{scope}' specified. Valid options are 'all', 'future', or 'past'.")
            raise ValueError(f"Invalid scope '{scope}' specified. Valid options are 'all', 'future', or 'past'.")

    @classmethod
    def from_defaults(cls, gcal_id: str) -> GoogleCalendarManager:
        """ Create a GoogleCalendarManager with default settings """
        auth = GoogleAuthManager()
        api = GoogleCalendarAPI(auth_manager=auth, calendar_id=gcal_id)
        return cls(api)

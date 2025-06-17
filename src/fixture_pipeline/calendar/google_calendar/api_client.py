import os
import time
import traceback
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from icalendar import Event

from . import logger
from .auth import GoogleAuthManager


class GoogleCalendarAPI:
    """ TODO """

    def __init__(self, auth_manager: GoogleAuthManager, calendar_id: str | None = None):
        """ TODO """
        self.auth_manager = auth_manager
        self.calendar_id = self._get_env_calendar_id(calendar_id)
        self.service = build('calendar', 'v3', credentials=self.auth_manager.credentials)

    def _get_env_calendar_id(self, calendar_id: str | None = None) -> str:
        """ TODO """
        gcal_id = os.getenv('GOOGLE_CALENDAR_ID')
        calendar_id = calendar_id or gcal_id
        if not calendar_id:
            logger.error("Google Calendar ID is not set. Please set it in the environment variable GOOGLE_CALENDAR_ID.")
            raise ValueError("Google Calendar ID is not set. Please set it in the environment variable GOOGLE_CALENDAR_ID.")
        return calendar_id

    def fetch_events(self, date_from: str, date_to: str) -> list:
        """ Fetch events from Google Calendar within a date range """
        try:
            events_result = self.service.events().list(
                calendarId=self.calendar_id,
                timeMin=date_from,
                timeMax=date_to,
                singleEvents=True,
                orderBy='startTime'
            ).execute()
            return events_result.get('items', [])
        except Exception as e:
            logger.error(f"Error fetching events: {e}")
            logger.error(traceback.format_exc())
            raise

    def add_event(self, event: Event, raise_on_error: bool = False) -> None:
        """ Add an event to Google Calendar """
        try:
            event_body = {
                'summary': event.get('summary'),
                'description': event.get('description'),
                'start': {
                    'dateTime': event.get('dtstart').dt.isoformat(),
                    'timeZone': event.get('dtstart').params.get('TZID', 'UTC')
                },
                'end': {
                    'dateTime': event.get('dtend').dt.isoformat(),
                    'timeZone': event.get('dtend').params.get('TZID', 'UTC')
                },
                'location': event.get('location'),
            }
            self.service.events().insert(calendarId=self.calendar_id, body=event_body).execute()
        except HttpError as e:
            logger.debug(f"Error adding event: {e}")
            if raise_on_error:
                raise
            if e.resp.status == 403 and 'rateLimitExceeded' in str(e):
                time.sleep(30)
                return self.add_event(event, raise_on_error=True)
            raise
        except Exception as e:
            logger.error(f"Error adding event: {e}")
            logger.error(traceback.format_exc())
            raise

    def delete_events(self, date_from: str, date_to: str) -> None:
        """ Delete events from Google Calendar within a date range """
        try:
            events = self.fetch_events(date_from, date_to)
            for event in events:
                self.delete_event(event['id'])
        except Exception as e:
            logger.error(f"Error deleting events: {e}")
            logger.error(traceback.format_exc())
            raise

    def delete_event(self, event_id: str) -> None:
        """ Delete a specific event from Google Calendar """
        try:
            self.service.events().delete(calendarId=self.calendar_id, eventId=event_id).execute()
        except Exception as e:
            logger.error(f"Error deleting event with ID {event_id}: {e}")
            logger.error(traceback.format_exc())
            raise

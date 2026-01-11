import time
from datetime import datetime
from icalendar import Event
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from . import logger
from .auth import GoogleAuthManager
from .console import TemporaryConsolePrinter


printer = TemporaryConsolePrinter()

class GoogleCalendarAPI:
    """ TODO """

    def __init__(self, auth_manager: GoogleAuthManager, calendar_id: str):
        """ TODO """
        self.auth_manager = auth_manager
        self.calendar_id = calendar_id
        self.service = build('calendar', 'v3', credentials=self.auth_manager.credentials)

        self._validate_cal_id()

    def _validate_cal_id(self) -> None:
        """ TODO """
        try:
            self.service.calendars().get(calendarId=self.calendar_id).execute()
        except Exception as e:
            logger.error(f"Invalid calendar ID: {e}")
            raise

    # Fetch events

    def fetch_events(self, date_from: str | None = None, date_to: str | None = None) -> list:
        """ Fetch events from Google Calendar within a date range """
        try:
            params = {
                'calendarId': self.calendar_id,
                'singleEvents': True,
                'orderBy': 'startTime',
                'maxResults': 2500
            }
            if date_from:
                params['timeMin'] = self._format_date(date_from)
            if date_to:
                params['timeMax'] = self._format_date(date_to)

            all_events = []
            page_token = None

            while True:
                if page_token:
                    params['pageToken'] = page_token
                events_result = self.service.events().list(**params).execute()
                items = events_result.get('items', [])
                all_events.extend(items)

                page_token = events_result.get('nextPageToken')
                if not page_token:
                    break

            return all_events
        except Exception:
            logger.exception(f"Unexpected error fetching events.")
            raise

    # Add events

    def add_events(
        self,
        events: list[Event],
        date_from: str | None = None,
        date_to: str | None = None,
        verbose: bool = False
    ) -> None:
        """ Add multiple events to Google Calendar """
        date_from = datetime.fromisoformat(date_from).date() if date_from else None
        date_to = datetime.fromisoformat(date_to).date() if date_to else None

        N_events = len(events)
        for i, event in enumerate(events):
            logger.debug(f"Adding event {i + 1}/{N_events}: {event.get('summary')}")
            if verbose:
                printer.print(f"Adding event {i + 1}/{N_events}: {event.get('summary')}")
            if date_from and event.get('dtstart').dt.date() < date_from:
                continue
            if date_to and event.get('dtend').dt.date() > date_to:
                continue
            self.add_event(event)
        if verbose:
            printer.clear()
        logger.info(f"Added {N_events} events to Google Calendar.")

    def add_event(self, event: Event) -> None:
        """ Add an event to Google Calendar """
        event_body = {
            'summary': event.get('summary'),
            'description': event.get('description'),
            'start': {
                'dateTime': event.get('dtstart').dt.isoformat(timespec="seconds"),
                'timeZone': event.get('dtstart').params.get('TZID', 'UTC')
            },
            'end': {
                'dateTime': event.get('dtend').dt.isoformat(timespec="seconds"),
                'timeZone': event.get('dtend').params.get('TZID', 'UTC')
            },
            'location': event.get('location'),
        }

        max_attempts = 5
        backoff = 1

        for attempt in range(max_attempts):
            try:
                self.service.events().insert(calendarId=self.calendar_id, body=event_body).execute()
                time.sleep(0.1)
                return
            except HttpError as e:
                logger.debug(f"Attempt {attempt + 1}: Error adding event: {e}")
                if e.resp.status == 403 and e.error_details and e.error_details[0].get('reason') == 'rateLimitExceeded':
                    if attempt < max_attempts - 1:
                        logger.warning(f"Rate limit exceeded. Retrying in {backoff} seconds...")
                        time.sleep(backoff)
                        backoff *= 2
                        continue
                    else:
                        logger.error("Max attempts reached. Could not add event due to rate limit.")
                        raise e
            except Exception:
                logger.exception("Unexpected error adding event.")
                raise

    # Delete events

    def delete_events(
        self,
        date_from: str | None = None,
        date_to: str | None = None,
        verbose: bool = False
    ) -> None:
        """ Delete events from Google Calendar within a date range """
        events = self.fetch_events(date_from, date_to)
        N_events = len(events)
        for i, event in enumerate(events):
            logger.debug(f"Deleting event {i + 1}/{N_events}: {event.get('summary')}")
            if verbose:
                printer.print(f"Deleting event {i + 1}/{N_events}")
            self.delete_event(event['id'])
        if verbose:
            printer.clear()
        logger.info(f"Deleted {N_events} events from Google Calendar.")

    def delete_event(self, event_id: str) -> None:
        """ Delete a specific event from Google Calendar """
        max_attempts = 5
        backoff = 1

        for attempt in range(max_attempts):
            try:
                self.service.events().delete(calendarId=self.calendar_id, eventId=event_id).execute()
                time.sleep(0.1)
                return
            except HttpError as e:
                logger.debug(f"Attempt {attempt + 1}: Error deleting event {event_id}: {e}")
                if e.resp.status == 403 and e.error_details and e.error_details[0].get('reason') == 'rateLimitExceeded':
                    if attempt < max_attempts - 1:
                        logger.warning(f"Rate limit exceeded. Retrying in {backoff} seconds...")
                        time.sleep(backoff)
                        backoff *= 2
                        continue
                    else:
                        logger.error("Max attempts reached. Could not delete event due to rate limit.")
                        raise e
            except Exception:
                logger.exception("Unexpected error deleting event.")
                raise

    # Helpe methods

    def _format_date(self, date: str) -> str:
        """ Format date to TODO format """
        try:
            dt = datetime.fromisoformat(date)
        except ValueError:
            dt = datetime.strptime(date, "%Y-%m-%d")
        return dt.replace(hour=0, minute=0, second=0).isoformat(timespec='seconds') + 'Z'

import time
import traceback
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from icalendar import Event

from . import logger
from .auth import GoogleAuthManager


class GoogleCalendarAPI:
    """ TODO """

    def __init__(self, auth_manager: GoogleAuthManager, calendar_id: str):
        """ TODO """
        self.auth_manager = auth_manager
        self.calendar_id = calendar_id
        self.service = build('calendar', 'v3', credentials=self.auth_manager.credentials)

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
                params['timeMin'] = date_from
            if date_to:
                params['timeMax'] = date_to

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
        except Exception as e:
            logger.error(f"Unexpected error fetching events: {e}")
            logger.debug("Traceback:\n%s", traceback.format_exc())
            raise

    def add_event(self, event: Event) -> None:
        """ Add an event to Google Calendar """
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
            except Exception as e:
                logger.error(f"Unexpected error adding event: {e}")
                logger.debug("Traceback:\n%s", traceback.format_exc())
                raise

    def delete_events(
        self,
        date_from: str | None = None,
        date_to: str | None = None,
        verbose: bool = False
    ) -> None:
        """ Delete events from Google Calendar within a date range """
        try:
            events = self.fetch_events(date_from, date_to)
            N_events = len(events)
            for i, event in enumerate(events):
                if verbose:
                    print(f"\r{' ' * 80}\rDeleting event {i + 1}/{N_events}", end='\r')
                self.delete_event(event['id'])
        except Exception as e:
            logger.error(f"Unexpected error deleting events: {e}")
            logger.debug("Traceback:\n%s", traceback.format_exc())
            raise

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
            except Exception as e:
                logger.error(f"Unexpected error deleting event: {e}")
                logger.debug("Traceback:\n%s", traceback.format_exc())
                raise

import base64
import logging
import time
from datetime import datetime

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from icalendar import Event

from .auth import GoogleAuthManager
from .console import TemporaryConsolePrinter

logger = logging.getLogger(__name__)


def google_event_id(source_id: str) -> str:
    """ A Google-legal event id derived from the provider's own id.

    Google accepts a caller-supplied id, but only in base32hex — lowercase `a`
    to `v` and the digits — so `mch:12345` cannot be used as it stands. base32
    encoding maps onto exactly that alphabet, is deterministic, and is
    reversible, so an event on the calendar can still be traced back to the
    fixture it came from.

    Verified against the live API: an event inserted under such an id is
    retrievable by it, and `mch:12345` yields `dlhmgehh68pj8d8`.
    """
    return base64.b32hexencode(source_id.encode()).decode().lower().rstrip("=")


def source_id_from_google_id(event_id: str) -> str | None:
    """ The provider id an event was written under, or None if we did not write it.

    Anything the user added to the calendar by hand decodes to nothing usable,
    and must be left alone by a sync.
    """
    padded = event_id.upper() + "=" * (-len(event_id) % 8)
    try:
        return base64.b32hexdecode(padded).decode()
    except (ValueError, UnicodeDecodeError):
        return None


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
            logger.exception("Unexpected error fetching events.")
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

        n_events = len(events)
        for i, event in enumerate(events):
            logger.debug(f"Adding event {i + 1}/{n_events}: {event.get('summary')}")
            if verbose:
                printer.print(f"Adding event {i + 1}/{n_events}: {event.get('summary')}")
            if date_from and event.get('dtstart').dt.date() < date_from:
                continue
            if date_to and event.get('dtend').dt.date() > date_to:
                continue
            self.add_event(event)
        if verbose:
            printer.clear()
        logger.info(f"Added {n_events} events to Google Calendar.")

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

        # The iCalendar event is mapped field by field, so anything not named
        # here never reaches Google. The UID carries the provider's event id,
        # and setting it as Google's own id is what lets a later sync recognise
        # this event instead of writing a second copy of the same fixture.
        uid = event.get('uid')
        if uid:
            event_body['id'] = google_event_id(str(uid))

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
                    logger.error("Max attempts reached. Could not add event due to rate limit.")
                    raise e

                if e.resp.status == 409 and event_body.get('id'):
                    # Google keeps an id reserved after the event is deleted, so
                    # inserting a fixture we have written before is refused
                    # outright. It can still be written through: `update`
                    # succeeds where `insert` will not, on a cancelled event and
                    # on a deleted one alike. This makes writing an event
                    # idempotent, which is what allows the same fixture to be
                    # rewritten every night without accumulating copies.
                    logger.debug(f"Event {event_body['id']} already exists; updating it in place.")
                    self.service.events().update(
                        calendarId=self.calendar_id, eventId=event_body['id'], body=event_body
                    ).execute()
                    time.sleep(0.1)
                    return

                # Previously this fell out of the `except` and round the loop,
                # so after five attempts the method returned as though it had
                # worked. Events went missing from the calendar with nothing
                # logged above debug.
                logger.error(f"Could not add event '{event_body.get('summary')}': {e}")
                raise
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
        n_events = len(events)
        for i, event in enumerate(events):
            logger.debug(f"Deleting event {i + 1}/{n_events}: {event.get('summary')}")
            if verbose:
                printer.print(f"Deleting event {i + 1}/{n_events}")
            self.delete_event(event['id'])
        if verbose:
            printer.clear()
        logger.info(f"Deleted {n_events} events from Google Calendar.")

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

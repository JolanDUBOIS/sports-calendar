import os, pytz
from datetime import datetime, timezone
from pathlib import Path

from google.oauth2.credentials import Credentials # type: ignore
from google.auth.transport.requests import Request # type: ignore
from google_auth_oauthlib.flow import InstalledAppFlow # type: ignore
from googleapiclient.discovery import build # type: ignore
from google.auth.exceptions import RefreshError # type: ignore
from icalendar import Calendar, Event # type: ignore

from src.calendar import logger
from src.config import get_config


class GoogleCalendarManager:
    """ TODO """
    
    SCOPES = ['https://www.googleapis.com/auth/calendar']
    
    def __init__(self, credentials_file_path: str = None, google_calendar_id: str = None):
        """ TODO """
        self._credentials_file_path = credentials_file_path
        self._google_calendar_id = google_calendar_id

    @property
    def credentials_file_path(self) -> str:
        """ TODO """
        if self._credentials_file_path is None:
            self._credentials_file_path = get_config('google.credentials_file_path')
            if self._credentials_file_path is None:
                raise ValueError('Google credentials file path is not set. Please set it in the config.')
        
        self._credentials_file_path = Path(self._credentials_file_path)
        if not self._credentials_file_path.exists():
            raise FileNotFoundError(f"Credentials file not found: {self._credentials_file_path}.")

        return self._credentials_file_path

    @property
    def credentials(self) -> Credentials:
        """ TODO """
        creds = None
        token_path = Path('credentials') / 'token.json'
        
        def write_token(token_path: Path, creds: Credentials):
            with token_path.open(mode='w') as token:
                token.write(creds.to_json())

        if token_path.exists():
            try:
                creds = Credentials.from_authorized_user_file(token_path, self.SCOPES)
                if creds and creds.valid:
                    return creds
            except Exception as e:
                logger.warning(f"Failed to load credentials: {e}")
                token_path.unlink(missing_ok=True)

        if creds and creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
                write_token(token_path, creds)
                return creds
            except RefreshError:
                logger.warning("Refresh token expired or revoked. Re-authenticating...")
                token_path.unlink(missing_ok=True)

        flow = InstalledAppFlow.from_client_secrets_file(
            self.credentials_file_path,
            self.SCOPES
        )
        creds = flow.run_local_server(port=0, open_browser=False)
        write_token(token_path, creds)

        return creds
    
    @property
    def google_calendar_id(self) -> str:
        """ TODO """
        if self._google_calendar_id is None:
            self._google_calendar_id = os.getenv("GOOGLE_CALENDAR_ID", "primary")
            if self._google_calendar_id == "primary":
                raise ValueError('GOOGLE_CALENDAR_ID environment variable is not set. Please set it to your Google Calendar ID.')
        return self._google_calendar_id

    def add_calendar(self, calendar: Calendar):
        """ TODO """
        logger.info("Adding events to Google Calendar...")
        
        creds = self.credentials
        service = build('calendar', 'v3', credentials=creds)

        existing_events = self.fetch_existing_events()
        logger.debug(f"Existing events: {existing_events}")
        paris_tz = pytz.timezone('Europe/Paris')

        for event in calendar.events:
            # Extract event details
            summary = event.get('SUMMARY', 'No title')
            location = event.get('LOCATION', 'No location')
            description = event.get('DESCRIPTION', 'No description')
            start_time = event.get('DTSTART').dt if event.get('DTSTART') else None
            end_time = event.get('DTEND').dt if event.get('DTEND') else None

            if not start_time or not end_time:
                logger.warning(f"Skipping event '{summary}' due to missing start or end time.")
                continue

            # Check if the event already exists
            if self.event_exists(start_time, summary, existing_events):
                logger.debug(f"Event '{summary}' already exists, skipping.")
                continue

            if start_time.tzinfo and start_time.tzinfo != paris_tz:
                start_time_naive = start_time.replace(tzinfo=None)
                start_time = paris_tz.localize(start_time_naive)

            if end_time.tzinfo and end_time.tzinfo != paris_tz:
                end_time_naive = end_time.replace(tzinfo=None)
                end_time = paris_tz.localize(end_time_naive)

            event_data = {
                'summary': summary,
                'location': location,
                'description': description,
                'start': {
                    'dateTime': start_time.isoformat(),
                    'timeZone': 'Europe/Paris',  # Adjust time zone if necessary
                },
                'end': {
                    'dateTime': end_time.isoformat(),
                    'timeZone': 'Europe/Paris',  # Adjust time zone if necessary
                },
            }

            # Add event to the specified calendar
            service.events().insert(
                calendarId=self.google_calendar_id,
                body=event_data
            ).execute()

        logger.info(f"ICS events added to calendar...")
    
    def fetch_existing_events(self) -> list:
        """ TODO """
        logger.info("Fetching Google Calendar...")
        
        creds = self.credentials
        service = build('calendar', 'v3', credentials=creds)
        
        now = datetime.now(timezone.utc).isoformat()
        events_result = service.events().list(
            calendarId=self.google_calendar_id,
            timeMin=now,
            maxResults=100,  # Adjust as needed
            singleEvents=True,
            orderBy='startTime'
        ).execute()
    
        events = events_result.get('items', [])
        existing_events = []

        for event in events:
            event_details = {
                'start_time': event.get('start', {}).get('dateTime'),
                'summary': event.get('summary'),
            }
            existing_events.append(event_details)

        return existing_events

    def delete_all_events(self):
        """ TODO """
        logger.warning("Deleting all events from Google Calendar...")
        
        creds = self.credentials
        service = build('calendar', 'v3', credentials=creds)
        
        events_result = service.events().list(calendarId=self.google_calendar_id, maxResults=2500, singleEvents=True).execute()
        events = events_result.get('items', [])
        
        if not events:
            logger.info("No events found.")
        else:
            for event in events:
                try:
                    service.events().delete(calendarId=self.google_calendar_id, eventId=event['id']).execute()
                except Exception as e:
                    logger.error(f"Failed to delete event: {e}")

    @staticmethod
    def event_exists(start_time: str, summary: str, existing_events: list) -> bool:
        """ Check if the event already exists in the calendar """
        for event in existing_events:
            if datetime.fromisoformat(event['start_time']).date() == start_time.date() and event['summary'] == summary:
                return True
        return False

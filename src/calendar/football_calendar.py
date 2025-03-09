import pytz, traceback
from pathlib import Path
from datetime import datetime, timedelta

import pandas as pd
from icalendar import Calendar, Event # type: ignore

from src.calendar import logger


class FootballCalendar:
    """ TODO - ics oriented """
    # TODO - Add a restriction (no past events and no events more than 2 months in the future)
    # TODO - Make class more robust (better error handling)

    def __init__(self, calendar: Calendar=None, max_future_days: int=48, **kwargs):
        """ TODO """
        self.limit_date = datetime.now() + timedelta(days=max_future_days)
        if calendar is None:
            self.calendar = Calendar()
        else:
            self.calendar = calendar

    @property
    def events(self):
        """ Expose events directly """
        return self.calendar.events

    @property
    def ics_content(self):
        """ TODO """
        return self.calendar.to_ical()

    def __add_event(
        self,
        title: str,
        utc_start: datetime,
        utc_end: datetime,
        location: str=None,
        description: str=None,
        **kwargs) -> None:
        """ TODO """
        assert isinstance(utc_start, datetime)
        assert isinstance(utc_end, datetime)

        event = Event()

        event.add('summary', title)
        utc_start = pytz.utc.localize(utc_start)
        event.add('dtstart', utc_start.astimezone(pytz.utc))
        utc_end = pytz.utc.localize(utc_end)
        event.add('dtend', utc_end.astimezone(pytz.utc))
        if location is not None:
            event.add('location', location)
        if description is not None:
            event.add('description', description)
        for key, value in kwargs:
            try:
                event.add(key, value)
            except Exception as e:
                logger.error(f"Couldn't add {value} to {key}.")
        
        self.calendar.add_component(event)
    
    def add_matches(self, matches: pd.DataFrame):
        """ TODO """
        for _, match in matches.iterrows():
            self.add_match(
                home_team=match['Home Team'],
                away_team=match['Away Team'],
                date=f"{match['Date']} {match['Time']}",
                competition=match['Competition']
            )

    def add_match(
        self,
        home_team: str,
        away_team: str,
        date: str,
        competition: str='',
        competition_code: str='',
        stage: str='',
        stadium: str=''
    ):
        """ TODO - We recommend to use the short name instead of the full name """
        try:
            if pd.isna(home_team) or pd.isna(away_team):
                logger.warning(f"Failed to add match: {home_team} - {away_team}.")
                return
            title = self.get_match_title(home_team, away_team, competition_code)
            match_start = datetime.strptime(date, "%Y-%m-%d %H:%M")
            if match_start > self.limit_date:
                logger.info(f"Match {title} is too far in the future. Skipping...")
                return
            match_end = match_start + timedelta(hours=2)
            location = stadium
            description = self.get_match_description(competition, stage)
            self.__add_event(
                title, 
                match_start,
                match_end, 
                location,
                description
            )
        except Exception as e:
            logger.debug(f"Failed to add match: {home_team} - {away_team}. {e}")
            logger.debug(f"date: {date}")
            logger.debug(traceback.format_exc())

    @staticmethod
    def get_match_title(
        home_team: str,
        away_team: str,
        competition_code: str='',
        **kwargs
    ) -> str:
        """ TODO """
        if competition_code == '':
            return f"{home_team} - {away_team}"
        else:
            return f"{home_team} - {away_team} ({competition_code})"
    
    @staticmethod
    def get_match_description(
        competition: str, 
        stage: str, 
        **kwargs
    ) -> str:
        """ TODO """
        description = f"Competition: {competition}"
        if stage != '':
            description += f"\nStage: {stage}"
        return description

    @staticmethod
    def format_stage(stage: str) -> str:
        """ TODO """
        return stage.lower().replace('_', ' ').capitalize()
    
    def save_to_ics(self, file_path: Path):
        """ TODO """
        file_suffix = file_path.suffix
        if file_suffix != '.ics':
            logger.error("The file path must have a suffix '.ics'. Please provide a valid .ics file.")
            return
        with file_path.open(mode='wb') as ics_file:
            ics_file.write(self.ics_content)

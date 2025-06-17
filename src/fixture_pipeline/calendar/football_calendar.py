from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path

import pandas as pd
from icalendar import Calendar, Event

from . import logger
from datetime import datetime, timedelta


@dataclass
class MatchDetails:
    home_team_name: str
    away_team_name: str
    date_time: str
    competition_name: str = None
    stage: str = None
    leg: str = None
    location: str = None

@dataclass
class MatchesDetails:
    matches: list[MatchDetails]

    def __iter__(self):
        return iter(self.matches)
    
    def __len__(self):
        return len(self.matches)
    
    def __getitem__(self, index):
        return self.matches[index]

    @classmethod
    def from_dataframe(cls, df: pd.DataFrame) -> MatchesDetails:
        """ Create MatchesDetails from a DataFrame """
        matches = []
        for _, row in df.iterrows():
            try:
                match = MatchDetails(
                    home_team_name=row['home_team_name'],
                    away_team_name=row['away_team_name'],
                    date_time=row['date_time'],
                    competition_name=cls.clean_value(row.get('competition_name')),
                    stage=cls.clean_value(row.get('stage')),
                    leg=cls.clean_value(row.get('leg')),
                    location=cls.clean_value(row.get('venue'))
                )
                matches.append(match)
            except Exception as e:
                logger.error(f"Error creating MatchDetails from row: {row}. Error: {e}")
                raise
        return cls(matches)

    @staticmethod
    def clean_value(val):
        return None if pd.isna(val) else val

class FootballCalendar:
    """ Football Calendar for managing football events in icalendar format """

    def __init__(self, calendar: Calendar = None):
        """ Initialize the FootballCalendar with an optional Calendar object """
        self.calendar = calendar if calendar else Calendar()

    def add_matches(self, matches: MatchesDetails) -> None:
        """ Add multiple matches to the calendar """
        for _match in matches:
            self.add_match(_match)

    def add_match(self, match: MatchDetails) -> None:
        """ Add a match to the calendar """
        event = Event()
        event.add('summary', f"{match.home_team_name} - {match.away_team_name}")
        
        start = datetime.strptime(match.date_time, "%Y-%m-%d %H:%M:%S%z")
        end = start + timedelta(hours=2)
        
        event.add('dtstart', start)
        event.add('dtend', end)

        description_lines = []
        if match.competition_name:
            description_lines.append(f"Competition: {match.competition_name}")
        if match.stage:
            description_lines.append(f"Stage: {match.stage}")
        if match.leg:
            description_lines.append(f"Leg: {match.leg}")
        if description_lines:
            event.add('description', "\n".join(description_lines))

        if match.location:
            event.add('location', match.location)

        self.calendar.add_component(event)

    def save_to_ics(self, path: Path) -> None:
        """ Save the calendar to an ICS file """
        path_suffix = path.suffix.lower()
        if path_suffix != '.ics':
            logger.error(f"Invalid file extension: {path_suffix}. Expected .ics")
            raise ValueError(f"Invalid file extension: {path_suffix}. Expected .ics")
        with open(path, 'wb') as f:
            f.write(self.calendar.to_ical())
        logger.info(f"Calendar saved to {path}")

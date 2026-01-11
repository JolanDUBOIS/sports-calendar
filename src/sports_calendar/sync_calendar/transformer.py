import pandas as pd

from . import logger
from .events import (
    SportsEventCollection,
    FootballEvent,
    F1Event
)
from sports_calendar.core.db import EventTableView, SPORT_SCHEMAS
from sports_calendar.core.utils import validate


# TODO:
# 1. Vectorize the transformation instead of iterating row by row.
# 2. Remove the use of "row" and use db columns instead (if possible).
class EventTransformer:
    """ Transformer for converting EventTableView to SportsEventCollection. """

    @classmethod
    def transform(cls, table: EventTableView) -> SportsEventCollection:
        """ Transform an EventTableView into a SportsEventCollection. """
        logger.debug(f"Transforming EventTableView for sport: {table.sport}")
        validate(table.sport in SPORT_SCHEMAS, f"Unsupported sport for transformation: {table.sport}", logger)

        events: SportsEventCollection = getattr(cls, f"_transform_{table.sport}")(table)
        events.drop_duplicates(inplace=True)
        return events

    @classmethod
    def _transform_football(cls, table: EventTableView) -> SportsEventCollection:
        """ Transform an EventTableView of football events into a SportsEventCollection. """
        validate(table.sport == 'football', "Table sport must be 'football' for this transformer.", logger)
        
        main_table = table.view.join(
            SPORT_SCHEMAS['football'].teams.view(),
            left_on='home_team_id',
            right_on='id',
            how='left',
            right_alias='home_team'
        ).join(
            SPORT_SCHEMAS['football'].teams.view(),
            left_on='away_team_id',
            right_on='id',
            how='left',
            right_alias='away_team'
        ).join(
            SPORT_SCHEMAS['football'].competitions.view(),
            left_on='competition_id',
            right_on='id',
            how='left',
            right_alias='competition'
        )

        events = []
        df = main_table.get()
        for _, row in df.iterrows():
            logger.debug(f"Transforming row: {row.to_dict()}")
            events.append(
                FootballEvent(
                    home_team_name=row['home_team.name'],
                    away_team_name=row['away_team.name'],
                    date_time=row['date'].isoformat(),
                    competition_name=cls.clean_value(row['competition.name']),
                    competition_abbreviation=cls.clean_value(row['competition.abbreviation']),
                    stage=cls.clean_value(row['stage']),
                    leg=cls.clean_value(row['leg']),
                    venue=cls.clean_value(row['venue'])
                )
            )
        
        return SportsEventCollection(events=events)

    @classmethod
    def _transform_f1(cls, table: EventTableView) -> SportsEventCollection:
        """ Transform an EventTableView of F1 events into a SportsEventCollection. """
        validate(table.sport == 'f1', "Table sport must be 'f1' for this transformer.", logger)

        events = []
        df = table.get()
        for _, row in df.iterrows():
            events.append(
                F1Event(
                    name=row['name'],
                    session=row['session_type'],
                    date_time=row['session_date'].isoformat(),
                    city=cls.clean_value(row['circuit_city']),
                    country=cls.clean_value(row['circuit_country'])
                )
            )

        return SportsEventCollection(events=events)

    @staticmethod
    def clean_value(val):
        """ Convert NaN or missing values to None. """
        return None if pd.isna(val) else val

from datetime import datetime

from ..base import BaseTable, Column
from sports_calendar.core.competition_stages import CompetitionStage


class FootballMatchesTable(BaseTable):
    """ Table for storing football match data. """
    __table__ = "football_matches"
    __file__ = "matches.csv"
    __sport__ = "football"

    id = Column(source="id", dtype=int)
    date = Column(source="date", dtype=datetime) # format="%Y-%m-%dT%H:%M:%S+00:00"
    competition_id = Column(source="competition_id", dtype=int)
    home_team_id = Column(source="home_team_id", dtype=int)
    away_team_id = Column(source="away_team_id", dtype=int)
    season_year = Column(source="season_year", dtype=int)
    stage = Column(source="stage", dtype=CompetitionStage, nullable=True)
    leg = Column(source="leg", dtype=int, nullable=True)
    leg_display = Column(source="leg_display", dtype=str, nullable=True)
    venue = Column(source="venue", dtype=str, nullable=True)

    # Dev fields
    staging_at = Column(source="staging_at", dtype=datetime, nullable=True)
    _ctime = Column(source="_ctime", dtype=datetime, nullable=True)

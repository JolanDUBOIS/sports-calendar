from datetime import datetime

from ..base import BaseTable, Column


class FootballTeamsTable(BaseTable):
    """ Table for storing football team data. """
    __table__ = "football_teams"
    __file__ = "teams.csv"
    __sport__ = "football"

    id = Column(source="id", dtype=int)
    name = Column(source="name", dtype=str)
    abbreviation = Column(source="abbreviation", dtype=str)
    display_name = Column(source="display_name", dtype=str)
    short_display_name = Column(source="short_display_name", dtype=str)
    location = Column(source="location", dtype=str)

    # Dev fields
    staging_at = Column(source="staging_at", dtype=datetime, nullable=True)
    _ctime = Column(source="_ctime", dtype=datetime, nullable=True)

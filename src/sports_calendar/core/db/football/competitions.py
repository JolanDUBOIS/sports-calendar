from ..base import BaseTable, Column


class FootballCompetitionsTable(BaseTable):
    """ Table for storing football competition data. """
    __table__ = "football_competitions"
    __file__ = "competitions.csv"
    __sport__ = "football"

    id = Column(source="id", dtype=int)
    name = Column(source="name", dtype=str)
    abbreviation = Column(source="abbreviation", dtype=str)
    short_name = Column(source=None, dtype=str)
    has_standings = Column(source="has_standings", dtype=bool)

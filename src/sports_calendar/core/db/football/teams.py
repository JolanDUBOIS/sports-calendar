from ..base import Table, Column


class FootballTeamsTable(Table):
    """ Table for storing football team data. """
    __file__ = "teams.csv"
    __sport__ = "football"
    id = Column(source="id", dtype="int")
    name = Column(source="name", dtype="str")
    abbreviation = Column(source="abbreviation", dtype="str")
    display_name = Column(source=None, dtype="str")
    short_display_name = Column(source=None, dtype="str")
    location = Column(source=None, dtype="str")

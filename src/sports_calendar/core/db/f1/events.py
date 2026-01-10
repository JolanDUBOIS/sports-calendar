from datetime import datetime

from ..base import Table, Column


class F1EventsTable(Table):
    """ Table for storing F1 event data. """
    __file__ = "events.csv"
    __sport__ = "f1"

    id = Column(source="session_id", dtype=int)
    name = Column(source="short_name", dtype=str)
    short_name = Column(source="short_name", dtype=str, nullable=True)
    season_year = Column(source="season_year", dtype=int)
    circuit_id = Column(source="circuit_id", dtype=int)
    circuit_name = Column(source="circuit_name", dtype=str)
    circuit_city = Column(source="circuit_city", dtype=str)
    circuit_country = Column(source="circuit_country", dtype=str)
    session_id = Column(source="session_id", dtype=int)
    session_date = Column(source="session_date", dtype=datetime, format="%Y-%m-%dT%H:%M:%S+00:00")
    session_type = Column(source="session_type", dtype=str)

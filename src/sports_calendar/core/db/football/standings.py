from ..base import Table, Column, Relationship
from .competitions import FootballCompetitionsTable
from .teams import FootballTeamsTable


class FootballStandingsTable(Table):
    __file__ = "standings.csv"
    __sport__ = "football"
    competition_id = Column(source="competition_id", dtype="int")
    team_id = Column(source="team_id", dtype="int")
    position = Column(source="position", dtype="int")
    points = Column(source="points", dtype="int")
    point_diff = Column(source="point_diff", dtype="int")
    matches_played = Column(source="matches_played", dtype="int")
    wins = Column(source="wins", dtype="int")
    draws = Column(source="draws", dtype="int")
    losses = Column(source="losses", dtype="int")
    goals_for = Column(source="goals_for", dtype="int")
    goals_against = Column(source="goals_against", dtype="int")
    deductions = Column(source="deductions", dtype="int")

    __relationships__ = [
        Relationship(FootballTeamsTable, local_key="team_id", alias="team"),
        Relationship(FootballCompetitionsTable, local_key="competition_id", alias="competition"),
    ]

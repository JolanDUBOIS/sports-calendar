from .base import BaseTable
from .f1 import F1EventsTable
from .football import (
    FootballMatchesTable,
    FootballStandingsTable,
    FootballTeamsTable,
    FootballCompetitionsTable,
)


class SportSchema:
    def __init__(
        self,
        *,
        sport: str,
        events: type[BaseTable],
        standings: type[BaseTable] | None = None,
        teams: type[BaseTable] | None = None,
        competitions: type[BaseTable] | None = None,
        players: type[BaseTable] | None = None
    ):
        self.sport = sport
        self.events = events
        self.standings = standings
        self.teams = teams
        self.competitions = competitions
        self.players = players

    def __str__(self) -> str:
        return (
            f"<SportSchema(sport={self.sport}, events={self.events.__table__}, " +
            f"standings={getattr(self.standings, '__table__', None)}, teams={getattr(self.teams, '__table__', None)}, " +
            f"competitions={getattr(self.competitions, '__table__', None)}, players={getattr(self.players, '__table__', None)})>"
        )

    def __repr__(self) -> str:
        return (
            f"<SportSchema(sport={self.sport}, events={self.events}, " +
            f"standings={self.standings}, teams={self.teams}, " +
            f"competitions={self.competitions}, players={self.players})>"
        )

SPORT_SCHEMAS: dict[str, SportSchema] = {
    "football": SportSchema(
        sport="football",
        events=FootballMatchesTable,
        standings=FootballStandingsTable,
        teams=FootballTeamsTable,
        competitions=FootballCompetitionsTable
    ),
    "f1": SportSchema(
        sport="f1",
        events=F1EventsTable
    )
}
import pandas as pd

from . import logger


class FRParser:
    """ parser for FootballRanking data. """

    @classmethod
    def parse_fr_fifa_ranking(cls, data: pd.DataFrame) -> pd.DataFrame:
        """ Parse FIFA football ranking data by extracting team name and code. """
        new_columns = data["team"].apply(cls._parse_fr_team).apply(pd.Series)
        data = pd.concat([data, new_columns], axis=1)
        return data

    @staticmethod
    def _parse_fr_team(team: str) -> dict:
        """ Extract team name and optional team code from a team string. """
        team = team.split(" (")
        team_name = team[0].strip()
        team_code = team[1].replace(")", "").strip() if len(team) > 1 else None
        return {
            "team_name": team_name,
            "team_code": team_code
        }

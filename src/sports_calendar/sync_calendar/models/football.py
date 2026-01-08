import pandas as pd

from .base import BaseTable


## Models

class FootballRegionsTable(BaseTable):
    """ Table for storing football region data. """
    __file_name__ = "regions.csv"
    __columns__ = None
    __sport__ = "football"

    @classmethod
    def query(cls, ids: list | None = None) -> pd.DataFrame:
        """ Query regions by IDs. """
        raise NotImplementedError("RegionsTable is not implemented yet.")

class FootballCompetitionsTable(BaseTable):
    """ Table for storing football competition data. """
    __file_name__ = "competitions.csv"
    __columns__ = {
        "id": {"type": "int", "source": "id"},
        "name": {"type": "str", "source": "name"},
        "abbreviation": {"type": "str", "source": "abbreviation"},
        "gender": {"type": "str", "source": None},
        "region_id": {"type": "int", "source": None},
        "type": {"type": "bool", "source": None},
        "has_standings": {"type": "bool", "source": "has_standings"}
    }
    __sport__ = "football"

    @classmethod
    def query(cls, ids: list | None = None) -> pd.DataFrame:
        """ Query competitions by IDs. """
        df = cls.get_table()
        if ids is not None:
            df = df[df['id'].isin(ids)]
        return df

class FootballTeamsTable(BaseTable):
    """ Table for storing football team data. """
    __file_name__ = "teams.csv"
    __columns__ = {
        "id": {"type": "int", "source": "id"},
        "name": {"type": "str", "source": "name"},
        "abbreviation": {"type": "str", "source": "abbreviation"},
        "gender": {"type": "str", "source": None},
        "venue": {"type": "str", "source": None},
        "league_id": {"type": "int", "source": None}
    }
    __sport__ = "football"

    @classmethod
    def query(cls, ids: list | None = None) -> pd.DataFrame:
        """ Query teams by IDs. """
        df = cls.get_table()
        if ids is not None:
            df = df[df['id'].isin(ids)]
        return df

class FootballMatchesTable(BaseTable):
    """ Table for storing football match data. """
    __file_name__ = "matches.csv"
    __columns__ = {
        "id": {"type": "int", "source": "id"},
        "competition_id": {"type": "int", "source": "competition_id"},
        "home_team_id": {"type": "int", "source": "home_team_id"},
        "away_team_id": {"type": "int", "source": "away_team_id"},
        "date_time": {"type": "str", "source": "date"},
        "venue": {"type": "str", "source": "venue"},
        "stage": {"type": "str", "source": "stage"},
        "leg": {"type": "int", "source": "leg"},
        "channels": {"type": "str", "source": None}
    }
    __sport__ = "football"

    @classmethod
    def get_table(cls) -> pd.DataFrame:
        """ Returns the matches table as a DataFrame. """
        df = super().get_table()
        temp_col = pd.to_datetime(df['date_time'], errors='coerce', utc=True)
        df['date'] = temp_col.dt.date # Bricolage...
        return df

    @classmethod
    def query(
        cls,
        ids: list | None = None,
        competition_ids: list | None = None,
        team_ids: list | None = None,
        date_from: str | None = None,
        date_to: str | None = None
    ) -> pd.DataFrame:
        """ Query matches by various filters. """
        df = cls.get_table()
        if ids is not None:
            df = df[df['id'].isin(ids)]
        if competition_ids is not None:
            df = df[df['competition_id'].isin(competition_ids)]
        if team_ids is not None:
            df = df[(df['home_team_id'].isin(team_ids)) | (df['away_team_id'].isin(team_ids))]
        if date_from is not None:
            df = df[df['date'] >= pd.to_datetime(date_from)]
        if date_to is not None:
            df = df[df['date'] <= pd.to_datetime(date_to)]
        df.drop(columns=['date'], inplace=True)
        return df.reset_index(drop=True)

class FootballStandingsTable(BaseTable):
    """ Table for storing football standings data. """
    __file_name__ = "standings.csv"
    __columns__ = {
        "id": {"type": "int", "source": None},
        "competition_id": {"type": "int", "source": "competition_id"},
        "team_id": {"type": "int", "source": "team_id"},
        "position": {"type": "int", "source": "position"},
        "points": {"type": "int", "source": "points"},
        "matches_played": {"type": "int", "source": "matches_played"},
        "wins": {"type": "int", "source": "wins"},
        "draws": {"type": "int", "source": "draws"},
        "losses": {"type": "int", "source": "losses"},
        "goals_for": {"type": "int", "source": "goals_for"},
        "goals_against": {"type": "int", "source": "goals_against"},
        "goal_difference": {"type": "int", "source": None}
    }
    __sport__ = "football"

    @classmethod
    def query(
        cls,
        competition_ids: list | None = None,
        team_ids: list | None = None
    ) -> pd.DataFrame:
        """ Query standings by competition and team IDs. """
        df = cls.get_table()
        if competition_ids is not None:
            df = df[df['competition_id'].isin(competition_ids)]
        if team_ids is not None:
            df = df[df['team_id'].isin(team_ids)]
        return df.reset_index(drop=True)


## Manager

class FootballMatchesManager:

    @staticmethod
    def query(
        ids: list | None = None,
        competition_ids: list | None = None,
        team_ids: list | None = None,
        date_from: str | None = None,
        date_to: str | None = None
    ) -> pd.DataFrame:
        """ Query football matches with context. """
        matches = FootballMatchesTable.query(
            ids=ids,
            competition_ids=competition_ids,
            team_ids=team_ids,
            date_from=date_from,
            date_to=date_to
        )
        teams_df = FootballTeamsTable.query().add_prefix('team_')
        competitions_df = FootballCompetitionsTable.query().add_prefix('competition_')

        full_matches = matches.merge(
            teams_df.add_prefix('home_'),
            left_on='home_team_id',
            right_on='home_team_id',
            how='left'
        ).merge(
            teams_df.add_prefix('away_'),
            left_on='away_team_id',
            right_on='away_team_id',
            how='left'
        ).merge(
            competitions_df,
            left_on='competition_id',
            right_on='competition_id',
            how='left'
        )

        return full_matches.reset_index(drop=True)

from abc import ABC, abstractmethod

import pandas as pd

from . import logger
from src import ROOT_PATH
from src.file_io import FileHandlerFactory, TrackedFileHandler
from src.config.manager import base_config


REPO_PATH = ROOT_PATH / "data/repository" / "staging" # TODO - TO CHANGE, NEED TO USE base_config FOR THIS !!!! - TODO

class BaseTable(ABC):
    """ Base class for all tables. """
    __file_name__ = None
    __columns__ = None
    file_handler: TrackedFileHandler = None

    @classmethod
    @abstractmethod
    def query(cls, **kwargs) -> pd.DataFrame:
        """ Abstract method to query data from the table. """
        raise NotImplementedError(f"{cls.__name__}.query() is not implemented.")

    @classmethod
    def get_table(cls) -> pd.DataFrame:
        """ Returns the table as a DataFrame. """
        if cls.__columns__ is None:
            logger.error(f"Columns for {cls.__name__} are not defined.")
            raise ValueError(f"Columns for {cls.__name__} are not defined.")
        if cls.file_handler is None:
            logger.error(f"File handler for {cls.__name__} is not initialized.")
            raise ValueError(f"File handler for {cls.__name__} is not initialized.")
        df = cls.file_handler.read()
        df = cls._as_types(df, cls.__columns__)
        cls._check_df(df)
        return df

    @staticmethod
    def _as_types(df: pd.DataFrame, columns: dict) -> pd.DataFrame:
        """ Convert DataFrame columns to specified types. """
        rename_map = {v["source"]: k for k, v in columns.items() if v["source"] is not None}
        df = df.rename(columns=rename_map)

        for col, props in columns.items():
            src = props.get("source")
            if col not in df.columns:
                if src is None:
                    df[col] = None
                    continue
                else:
                    logger.error(f"Column '{col}' not found in DataFrame.")
                    raise ValueError(f"Column '{col}' not found in DataFrame.")

            col_type = props.get("type")
            if not col_type:
                logger.warning(f"Column '{col}' has no type specified, skipping conversion.")
                continue

            # logger.debug(f"Converting column '{col}' to type '{col_type}'")
            try:
                if col_type == "int":
                    df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
                elif col_type == "float":
                    df[col] = pd.to_numeric(df[col], errors="coerce").astype(float)
                elif col_type == "bool":
                    df[col] = df[col].map({'True': True, 'False': False})
                    df[col] = df[col].astype("boolean")  # Pandas nullable bool
                else:
                    df[col] = df[col].astype(col_type)
            except Exception as e:
                logger.error(f"Error converting column '{col}' to {col_type}: {e}")
                raise e
        
        return df[[col for col in columns if col in df]]

    @staticmethod
    def _check_df(df: pd.DataFrame) -> None:
        """ Check if DataFrame is empty and raise an error if it is. """
        if df.empty:
            logger.error("DataFrame is empty.")
            raise ValueError("DataFrame is empty.")
        return None

class RegionsTable(BaseTable):
    """ TODO """
    __file_name__ = "regions.csv"
    __columns__ = None
    file_handler = FileHandlerFactory.create_file_handler(REPO_PATH / __file_name__, tracked=True)

    @classmethod
    def query(cls, ids: list = None) -> pd.DataFrame:
        """ TODO """
        raise NotImplementedError("RegionsTable is not implemented yet.")

class CompetitionsTable(BaseTable):
    """ Table for storing competition data. """
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
    file_handler = FileHandlerFactory.create_file_handler(REPO_PATH / __file_name__, tracked=True)

    @classmethod
    def query(cls, ids: list = None) -> pd.DataFrame:
        """ Query competitions by IDs. """
        df = cls.get_table()
        if ids is not None:
            df = df[df['id'].isin(ids)]
        return df

class TeamsTable(BaseTable):
    """ Table for storing team data. """
    __file_name__ = "teams.csv"
    __columns__ = {
        "id": {"type": "int", "source": "id"},
        "name": {"type": "str", "source": "name"},
        "abbreviation": {"type": "str", "source": "abbreviation"},
        "gender": {"type": "str", "source": None},
        "venue": {"type": "str", "source": None},
        "league_id": {"type": "int", "source": None}
    }
    file_handler = FileHandlerFactory.create_file_handler(REPO_PATH / __file_name__, tracked=True)

    @classmethod
    def query(cls, ids: list = None) -> pd.DataFrame:
        """ Query teams by IDs. """
        df = cls.get_table()
        if ids is not None:
            df = df[df['id'].isin(ids)]
        return df

class MatchesTable(BaseTable):
    """ Table for storing match data. """
    __file_name__ = "matches.csv"
    __columns__ = {
        "id": {"type": "int", "source": "id"},
        "competition_id": {"type": "int", "source": "competition_id"},
        "home_team_id": {"type": "int", "source": "home_team_id"},
        "away_team_id": {"type": "int", "source": "away_team_id"},
        "date_time": {"type": "str", "source": "date_time_cet"},
        "venue": {"type": "str", "source": "venue"},
        "stage": {"type": "str", "source": "stage"},
        "leg": {"type": "int", "source": "leg"},
        "channels": {"type": "str", "source": None}
    }
    file_handler = FileHandlerFactory.create_file_handler(REPO_PATH / __file_name__, tracked=True)

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
        ids: list = None,
        competition_ids: list = None,
        team_ids: list = None,
        date_from: str = None,
        date_to: str = None
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

class StandingsTable(BaseTable):
    """ Table for storing standings data. """
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
    file_handler = FileHandlerFactory.create_file_handler(REPO_PATH / __file_name__, tracked=True)

    @classmethod
    def query(
        cls,
        competition_ids: list = None,
        team_ids: list = None
    ) -> pd.DataFrame:
        """ Query standings by competition and team IDs. """
        df = cls.get_table()
        if competition_ids is not None:
            df = df[df['competition_id'].isin(competition_ids)]
        if team_ids is not None:
            df = df[df['team_id'].isin(team_ids)]
        return df.reset_index(drop=True)

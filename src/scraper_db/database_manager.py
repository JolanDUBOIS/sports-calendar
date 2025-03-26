import os, atexit, hashlib
from datetime import datetime
from typing import Callable
from pathlib import Path

import pandas as pd

from src.scraper_db import logger


class DatabaseManager:
    """ TODO """

    TABLES_FILES = {
        "matches": {
            "filename": "matches.csv",
            "columns": ['Title', 'Date', 'SL Time', 'Time', 'Original Time (SL TZ)', 'Home Team', 'Away Team', 'Competition', 'Region', 'Channels', 'created_at']
        },
        "standings": {
            "filename": "standings.csv",
            "columns": ['Competition', 'Position', 'Team', 'Matches Played', 'Wins', 'Draws', 'Losses', 'Goals For', 'Goals Against', 'Goal Difference', 'Points', 'created_at']
        }
    }

    def __init__(self):
        """ TODO """
        self._folder_path = None
        self.__load_tables()
        atexit.register(self.__save_tables)

    @property
    def folder_path(self):
        """ TODO """
        if self._folder_path is None:
            self._folder_path = os.getenv("DATABASE_PATH")
            if self._folder_path is None:
                logger.error("DATABASE_PATH environment variable not set")
                raise ValueError("DATABASE_PATH environment variable not set")
            try:
                self._folder_path = Path(self._folder_path)
            except Exception as e:
                logger.error(f"Error while creating Path object from DATABASE_PATH: {e}")
            if not self._folder_path.exists():
                logger.error("DATABASE_PATH folder does not exist")
                raise ValueError("DATABASE_PATH folder does not exist")
        return self._folder_path

    def __load_tables(self):
        """ TODO """
        logger.debug("Loading tables")
        self.tables = {}
        for table, table_info in self.TABLES_FILES.items():
            file_path = self.folder_path / table_info["filename"]
            if not file_path.exists():
                logger.warning(f"Table file {file_path} does not exist, creating empty table")
                self.tables[table] = pd.DataFrame(columns=table_info["columns"])
            else:
                self.tables[table] = pd.read_csv(file_path)
            # TODO - check the columns

    def __save_tables(self):
        """ TODO """
        logger.debug("Saving tables")
        for table, table_info in self.TABLES_FILES.items():
            file_path = self.folder_path / table_info["filename"]
            self.tables[table].to_csv(file_path, index=False)

    def query(self, table: str, conditions: Callable[[pd.Series], bool]=None, latest_version: bool=False) -> pd.DataFrame:
        """ TODO """
        if table not in self.tables:
            logger.error(f"Table {table} does not exist.")
            raise ValueError(f"Table {table} does not exist.")
        if conditions is None:
            return self.tables[table]
        if latest_version:
            data = self.tables[table].loc[self.tables[table].groupby('id')['created_at'].idxmax()].reset_index(drop=True)
        else:
            data = self.tables[table]
        return data[data.apply(conditions, axis=1)]

    def insert(self, table: str, data: pd.DataFrame):
        """ TODO """
        if table not in self.tables:
            logger.error(f"Table {table} does not exist.")
            raise ValueError(f"Table {table} does not exist.")
        if data.empty:
            logger.debug(f"No data to insert in table {table}")
            return
        data["created_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S") # Add versioning
        if table == "matches":
            self._insert_matches(data)
        elif table == "standings":
            self._insert_standings(data)
        else:
            logger.error(f"Table {table} not supported.")
            raise ValueError(f"Table {table} not supported.")
    
    def _insert_matches(self, matches: pd.DataFrame):
        """ TODO """
        matches["id"] = matches.apply(lambda row: self._generate_id(row, columns=['Date', 'Competition', 'Home Team', 'Away Team']), axis=1)
        matches["cancelled"] = False

        all_matches = pd.concat([self.tables["matches"], matches], ignore_index=True)
        all_matches["created_at"] = pd.to_datetime(all_matches["created_at"], format="%Y-%m-%d %H:%M:%S")
        all_matches["date_time"] = pd.to_datetime(all_matches["Date"] + " " + all_matches["Time"], format="%Y-%m-%d %H:%M")

        now = datetime.now()        
        version_mask = all_matches.index.isin(all_matches.groupby('id')['created_at'].idxmax())
        all_matches['time_to_created'] = (now - all_matches['created_at']).dt.days
        all_matches.loc[version_mask & (all_matches['time_to_created'] > 1), 'cancelled'] = True

        self.tables['matches'] = all_matches

    def _insert_standings(self, standings: pd.DataFrame):
        """ TODO """
        standings["id"] = standings.apply(lambda row: self._generate_id(row, columns=['Competition', 'Team']), axis=1)
        self.tables['standings'] = pd.concat([self.tables['standings'], standings], ignore_index=True, join='outer')

    def _generate_id(self, row: pd.Series, columns: list[str]) -> str:
        """ TODO """
        str_id = "_".join([f"{row[col]}" for col in columns])
        return hashlib.md5(str_id.encode()).hexdigest()

    def delete(self, table: str, conditions: Callable[[pd.Series], bool]):
        """ TODO """
        if table not in self.tables:
            logger.error(f"Table {table} does not exist.")
            raise ValueError(f"Table {table} does not exist.")
        new_table = self.tables[table][~self.tables[table].apply(conditions, axis=1)]
        self.tables[table] = new_table

    def close(self):
        """ TODO """
        raise NotImplementedError("close method not implemented yet")

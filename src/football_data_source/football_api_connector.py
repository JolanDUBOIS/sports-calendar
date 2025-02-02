import os, json, time, traceback
from pathlib import Path
from datetime import datetime, timedelta
from collections import defaultdict
from typing import Union, Tuple, Dict, List

import requests
import numpy as np
import pandas as pd

from src.utils import concatenate_unique_rows
from src.football_data_source import logger


TIME_RANGE = int(os.getenv('TIME_RANGE', 21))   # TODO - better way to set default value

class FootballDataConnector:
    """ TODO """

    sport = 'football'
    api_url = "https://api.football-data.org"
    config_path = Path(__file__).resolve().parent / 'football_conn_config.json'

    def __init__(self, api_token: str=None, **kwargs):
        """ TODO """
        if api_token is None:
            self.api_token = self.__get_api_token()
        else:
            self.api_token = api_token

        self.config = self.__get_config()

    def __get_api_token(self) -> str:
        """ TODO """
        try:
            return os.getenv('FOOTBALL_DATA_API_TOKEN').strip()
        except AttributeError:
            error_msg = "Environment variable FOOTBALL_DATA_API_TOKEN hasn't been correctly defined."
            logger.error(error_msg)
            raise ValueError(error_msg)
    
    def __get_config(self) -> Dict[str, dict]:
        """ TODO """
        with self.config_path.open(mode='r') as file:
            config = json.load(file)
        return config

    def request_competitions(self) -> pd.DataFrame:
        """ TODO - Not to be called every time """
        request_name = 'competitions'
        response_df = self.make_request(request_name=request_name)
        return response_df
    
    def request_areas(self) -> pd.DataFrame:
        """ TODO - Not to be called every time """
        request_name = 'areas'
        response_df = self.make_request(request_name=request_name)
        return response_df

    def request_competition_standings(self, competition_id: int) -> pd.DataFrame:
        """ TODO """
        request_name = 'competitions_standings'
        response_df = self.make_request(
            request_name=request_name,
            url_params={'id': competition_id}
        )
        return response_df

    def request_competition_teams(self, competition_id: int) -> pd.DataFrame:
        """ TODO """
        request_name = 'competitions_teams'
        response_df = self.make_request(
            request_name=request_name,
            url_params={'id': competition_id}
        )
        return response_df

    def request_team_matches(
        self,
        team_id: int,
        date_from: str=None,
        date_to: str=None
    ) -> pd.DataFrame:
        """ TODO """
        request_name = 'teams_matches'
        params = {"dateFrom": date_from, "dateTo": date_to}
        response_df = self.make_request(
            request_name=request_name,
            url_params={'id': team_id},
            params=params
        )
        return response_df

    def request_upcoming_team_matches(
        self,
        team_id: int,
        time_range: int=TIME_RANGE
    ) -> pd.DataFrame:
        """ TODO """
        start_date, end_date = self.get_date_range(time_range)
        matches = self.request_team_matches(
            team_id=team_id,
            date_from=start_date,
            date_to=end_date
        )
        return matches

    def request_competition_matches(
        self,
        competition_id: int,
        date_from: str=None,
        date_to: str=None
    ) -> pd.DataFrame:
        """ TODO """
        request_name = 'competitions_matches'
        params = {"dateFrom": date_from, "dateTo": date_to}
        response_df = self.make_request(
            request_name=request_name,
            url_params={'id': competition_id},
            params=params
        )
        return response_df
    
    def request_upcoming_competition_matches(
        self,
        competition_id: int,
        time_range: int=TIME_RANGE
    ) -> pd.DataFrame:
        """ TODO """
        start_date, end_date = self.get_date_range(time_range)
        matches = self.request_competition_matches(
            competition_id=competition_id,
            date_from=start_date,
            date_to=end_date
        )
        return matches

    def request_teams(self) -> pd.DataFrame:
        """ TODO """
        competitions = self.request_competitions()
        teams = pd.DataFrame()
        for competition_id in competitions['id'].to_list():
            new_teams = self.request_competition_teams(competition_id)
            teams = concatenate_unique_rows(teams, new_teams)
        return teams
    
    def make_request(
        self,
        request_name: str,
        url_params: Dict[str, any]=None,
        params: Dict[str, any]=None
    ) -> pd.DataFrame:
        """ TODO """
        if url_params is None:
            url_params = {}
        if params is None:
            params = {}

        queries_config = self.config["queries"]
        request_config = self.config["requests"].get(request_name)
        url_fragment = request_config.get("url_fragment").format(**url_params)
        needs_multiple_requests = request_config.get("multiple_requests")
        query_name = request_config.get("query")
        
        if needs_multiple_requests:
            response_df = pd.DataFrame()
            offset = 0
            cpt = 0
            while True:
                cpt += 1
                params.update({'offset': offset})
                response = self.make_request_api(
                    api_url=self.api_url,
                    api_token=self.api_token,
                    url_fragment=url_fragment,
                    params=params
                )
                offset += response.get('filters').get('limit')
                new_data = DataExtractor.extract(query_name, queries_config, response)
                if new_data.empty:
                    return response_df
                response_df = concatenate_unique_rows(response_df, new_data)
                if cpt > 10:
                    raise RuntimeError

        else:
            response = self.make_request_api(
                api_url=self.api_url,
                api_token=self.api_token,
                url_fragment=url_fragment,
                params=params
            )

            if response is None:
                return pd.DataFrame()

            return DataExtractor.extract(query_name, queries_config, response)
    
    @staticmethod
    def make_request_api(
        api_url: str,
        api_token: str,
        url_fragment: str,
        params: Dict[str, str],
        sleep_time: int=60
    ) -> Dict[str, any]:
        """ TODO """
        url = api_url + url_fragment
        headers = {
            'X-Auth-Token': api_token,
            'Content-Type': 'application/json'
        }

        try:
            logger.info(f"Requesting URL - {url}")
            response = requests.get(url=url, params=params, headers=headers)

            if response.status_code == 200:
                data = response.json()
                return data

            elif response.status_code == 429 and sleep_time is not None:
                # Too many requests
                logger.info(f"Too many requests. Waiting {sleep_time}s.")
                time.sleep(sleep_time)
                return FootballDataConnector.make_request_api(
                    api_url=api_url,
                    api_token=api_token,
                    url_fragment=url_fragment,
                    params=params,
                    sleep_time=None
                )

            else:
                logger.error(f"Request failed - Status code {response.status_code} - Reason {response.reason}")
                return None

        except requests.RequestException as e:
            logger.error(f"An error occured while requesting : {e}")
            logger.debug(traceback.format_exc())
            return None
    
    @staticmethod
    def get_date_range(time_range: int=TIME_RANGE) -> Tuple[str, str]:
        """ TODO """
        start_date = datetime.now()
        end_date = (start_date + timedelta(days=time_range)).strftime('%Y-%m-%d')
        start_date = start_date.strftime('%Y-%m-%d')
        return start_date, end_date

class DataExtractor:
    """ TODO """

    @classmethod
    def extract(
        cls,
        query_name: str,
        queries_config: Dict[str, List[dict]],
        api_response: dict
    ) -> pd.DataFrame:
        """ TODO """
        query_config = queries_config[query_name]
        extracted_data = {}

        for query_set in query_config:
            data = cls.read_path(
                query_set=query_set,
                api_response=api_response
            )
            extracted_data.update(data)
        
        df = pd.DataFrame(extracted_data).replace(np.nan, None)
        return df

    @classmethod
    def read_path(
        cls,
        query_set: Dict[str, Union[list, dict]],
        api_response: dict
    ) -> Dict[str, list]:
        """ TODO """
        path = query_set.get("path")
        data_keys = query_set.get("data_keys")
        data = defaultdict(list)

        pointers = cls.search(
            path=path,
            data=api_response
        )

        for pointer in pointers:
            for data_key, key in data_keys.items():
                data[data_key].append(pointer.get(key))
        
        return data

    @classmethod
    def search(
        cls,
        path: List[Union[str, dict]],
        data: Union[dict, list]
    ) -> List[Dict[str, any]]:
        """ TODO - Recursive """
        if len(path) == 0:
            return [data]

        path_element = path[0]
        if isinstance(path_element, str):
            if path_element == "*":
                if not isinstance(data, list):
                    error_msg = f"Data type should be a list for path element '*' not {type(data)}."
                    logger.error(error_msg)
                    raise TypeError(error_msg)

                pointers = []
                for data_element in data:
                    pointers.extend(cls.search(path[1:], data_element))
                return pointers

            else:
                if not isinstance(data, dict):
                    error_msg = f"Data type should be a dict for string path element not {type(data)}."
                    logger.error(error_msg)
                    raise TypeError(error_msg)
                
                return cls.search(path[1:], data.get(path_element))

        elif isinstance(path_element, dict):
            if not isinstance(data, list):
                error_msg = f"Data type should be a list for dict path element not {type(data)}."
                logger.error(error_msg)
                raise TypeError(error_msg)
            
            key = path_element.get("key")
            filter_key = path_element.get("filter_key")
            filter_value = path_element.get("filter_value")

            for data in data:
                if data.get(filter_key) == filter_value:
                    return cls.search(path[1:], data.get(key))

            return []
        
        else:
            error_msg = "Variable path_element must be either a dict or a str."
            logger.error(error_msg)
            raise TypeError(error_msg)

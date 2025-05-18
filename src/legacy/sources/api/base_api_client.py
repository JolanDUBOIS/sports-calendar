import time
import traceback
from typing import Any

import requests
import pandas as pd

from src.sources import BaseSourceClient, logger


class BaseApiClient(BaseSourceClient):
    """ TODO """

    def __init__(self, **kwargs):
        """ TODO """
        super().__init__(**kwargs)

    @property
    def base_url(self) -> str:
        """ Getter for the base_url property. """
        pass

    @property
    def source_name(self):
        """Getter for the source_name property."""
        pass

    def query_api(self, url: str, params: dict = None, headers: dict = None, max_retries: int = 3, delay: int = 5) -> dict|None:
        """ TODO """
        retries = 0
        while retries < max_retries:
            try:
                response = requests.get(url, params=params, headers=headers)

                if response.status_code == 200:
                    return response.json()
                elif response.status_code == 429:
                    logger.warning(f"Rate limit exceeded (status code 429). Attempt {retries}/{max_retries}. Retrying in {delay} seconds...")
                    time.sleep(delay)
                    retries += 1
                else:
                    logger.error(f"Failed to fetch data from URL: {url}. Status code: {response.status_code}")
                    return None

            except Exception as e:
                logger.error(f"Failed to fetch data from URL: {url}. Error: {e}")
                logger.debug(traceback.format_exc())
                break

        if retries >= max_retries:
            logger.error(f"Max retries reached for URL: {url}. Failed to fetch data.")

        return None

    def get_matches(
        self,
        competition_id: str = None,
        team_id: str = None,
        date_from: str = None,
        date_to: str = None,
        **kwargs
    ) -> pd.DataFrame:
        """ TODO """
        return pd.DataFrame()

    def get_standings(self, league_id: str, date: str = None, **kwargs) -> pd.DataFrame:
        """ TODO """
        return pd.DataFrame()

    @staticmethod
    def get_value_from_json(data: dict, path: list[str, int] | None) -> Any | None:
        """
        Retrieves a value from a nested JSON structure (dict or list) based on a given path.

        The function traverses the `data` dictionary or list using the provided `path`. The path is a list of keys 
        (strings) and/or indices (integers), where each element in the path corresponds to a step in the 
        nested structure. If a key or index does not match the expected type, an error message is logged, 
        and `None` is returned.

        Args:
            data (dict): The JSON-like structure (dictionary) from which to retrieve the value.
            path (list[str, int] | None): A list of keys (strings) and/or indices (integers).

        Returns:
            Any: The value at the given path in the data structure, or `None` if the path is invalid or does not exist.
        """
        if path is None:
            return None

        value = data.copy()
        for key in path:
            if value is None:
                # TODO - Error msg?
                return None
            if isinstance(key, str):
                if isinstance(value, dict):
                    value = value.get(key)
                else:
                    logger.error(f"Expected a dict at key '{key}', but got {type(value)}.")
                    return None
            elif isinstance(key, int):
                if isinstance(value, list):
                    value = value[key]
                else:
                    logger.error(f"Expected a list at index '{key}', but got {type(value)}.")
                    return None

        return value

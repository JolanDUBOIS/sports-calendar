import pandas as pd

from . import logger
from .processor_base_class import Processor
from src.clients import (
    ESPNApiClient,
    FootballDataApiClient,
    LiveSoccerScraper,
    FootballRankingScraper
)


class ClientProcessor(Processor):
    """ TODO - Used to fetch data from client sources & ingest it into landing... """

    sources = {
        "espn": ESPNApiClient,
        "football-data": FootballDataApiClient,
        "livesoccertv": LiveSoccerScraper,
        "football-ranking": FootballRankingScraper
    }

    def _run(self, source: str, method: str, params: list[dict] = None, **kwargs) -> list[dict] | pd.DataFrame:
        """ TODO """
        logger.info(f"Running ClientProcessor for source: {source} and method: {method}")
        client_class = self.sources.get(source)
        if client_class is None:
            logger.error(f"Client {source} not found.")
            raise ValueError(f"Client {source} not found.")
        client_obj = client_class()
        if params is None:
            params = [{}]
        return self.fetch_data_from_client(client_obj, method, params)

    def fetch_data_from_client(self, client_obj: any, method_name: str, params_list: list[dict]) -> list[dict]|pd.DataFrame|None:
        """ TODO """
        logger.debug(f"Fetching data from client: {client_obj.__class__.__name__}, method: {method_name}, params: {params_list}")
        method = getattr(client_obj, method_name, None)
        logger.debug(f"Method found: {method}")
        if method is None:
            raise ValueError(f"Method {method_name} not found in the class {client_obj.__class__.__name__}.")

        outputs = None
        for params in params_list:
            output = method(**params)
            outputs = self._append_data(outputs, output)

        logger.debug(f"Outputs: {outputs.head(5) if isinstance(outputs, pd.DataFrame) else outputs[:5]}")
        return outputs

    @staticmethod
    def _append_data(data: list[dict]|pd.DataFrame|None, new_data: dict|list[dict]|pd.DataFrame|None) -> list[dict]|pd.DataFrame:
        """ TODO """
        if data is None:
            if isinstance(new_data, pd.DataFrame):
                data = pd.DataFrame()
            elif isinstance(new_data, list) or isinstance(new_data, dict):
                data = []
        
        if new_data is None:
            logger.debug("New data is None, returning existing data.")
            return data

        if isinstance(data, pd.DataFrame):
            if isinstance(new_data, pd.DataFrame):
                data = pd.concat([data, new_data], ignore_index=True)
            else:
                logger.error(f"New data is not a DataFrame: {new_data}")
                raise TypeError(f"New data is not a DataFrame: {new_data}")

        elif isinstance(data, list):
            if isinstance(new_data, list):
                if all(isinstance(d, dict) for d in new_data):
                    data.extend(new_data)
                else:
                    logger.error(f"New data is not a list of dictionaries: {new_data}")
                    raise TypeError(f"New data is not a list of dictionaries: {new_data}")
            elif isinstance(new_data, dict):
                data.append(new_data)
            else:
                logger.error(f"New data is not a list or a dict: {new_data}")
                raise TypeError(f"New data is not a list or a dict: {new_data}")
        else:
            logger.error(f"Unsupported data type: {type(data)}")
            raise TypeError(f"Unsupported data type: {type(data)}")
        return data
import pandas as pd

from . import logger
from .processor_base_class import Processor
from src.clients import (
    ESPNApiClient,
    FootballDataApiClient,
    LiveSoccerScraper,
    FootballRankingScraper
)
from ...types import IOContent
from ...utils import concat_io_content


class ClientProcessor(Processor):
    """ Used to fetch data from client sources and ingest it into the landing zone. """

    sources = {
        "espn": ESPNApiClient,
        "football-data": FootballDataApiClient,
        "livesoccertv": LiveSoccerScraper,
        "football-ranking": FootballRankingScraper
    }

    def _run(self, source: str, method: str, params: list[dict] = None, **kwargs) -> IOContent:
        """ Run the processor by invoking the specified method from the given client source. """
        logger.info(f"Running ClientProcessor for source: {source} and method: {method}")
        client_class = self.sources.get(source)
        if client_class is None:
            logger.error(f"Client {source} not found.")
            raise ValueError(f"Client {source} not found.")
        client_obj = client_class()
        if params is None:
            params = [{}]
        return self.fetch_data_from_client(client_obj, method, params)

    def fetch_data_from_client(self, client_obj: any, method_name: str, params_list: list[dict]) -> IOContent:
        """ Call the client's method multiple times using the given parameters and aggregate results. """
        logger.debug(f"Fetching data from client: {client_obj.__class__.__name__}, method: {method_name}, params: {params_list}")
        method = getattr(client_obj, method_name, None)
        logger.debug(f"Method found: {method}")
        if method is None:
            raise ValueError(f"Method {method_name} not found in the class {client_obj.__class__.__name__}.")

        outputs = None
        for params in params_list:
            output = method(**params)
            outputs = concat_io_content(outputs, output)

        logger.debug(f"Outputs: {outputs.head(5) if isinstance(outputs, pd.DataFrame) else outputs[:5]}")
        return outputs

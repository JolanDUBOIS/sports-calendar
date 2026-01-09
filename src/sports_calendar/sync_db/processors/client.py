from __future__ import annotations
from typing import TYPE_CHECKING, Any

from . import logger
from .base import Processor
from sports_calendar.sync_db.clients import (
    ESPNApiClient,
    FootballDataApiClient,
    LiveSoccerScraper,
    FootballRankingScraper
)
from sports_calendar.sync_db.utils import concat_io_content

if TYPE_CHECKING:
    from sports_calendar.sc_core import IOContent
    from sports_calendar.sync_db.definitions.specs import ProcessingIOInfo
    

CLIENT_CLASS_REGISTRY = {
    "ESPNApiClient": ESPNApiClient,
    "FootballDataApiClient": FootballDataApiClient,
    "LiveSoccerScraper": LiveSoccerScraper,
    "FootballRankingScraper": FootballRankingScraper
}

class ClientProcessor(Processor):
    """ TODO """
    config_filename = "client"

    @classmethod
    def _run(cls, io_info: ProcessingIOInfo, params: list[dict[str, Any]] | None = None, **kwargs) -> list[dict]:
        """ TODO - it's a landing processor, so it doesn't need data... """
        config = cls.load_config(io_info.config_key)
        client_class_name = config.get("client_class")
        method_name = config.get("method")

        if client_class_name not in CLIENT_CLASS_REGISTRY:
            logger.error(f"Client class {client_class_name} not found in registry.")
            raise ValueError(f"Client class {client_class_name} not found in registry.")

        client_class = CLIENT_CLASS_REGISTRY[client_class_name]
        client_obj = client_class()

        if params is None:
            params = [{}]

        return cls.fetch_data_from_client(client_obj, method_name, params)

    @staticmethod
    def fetch_data_from_client(client_obj: any, method_name: str, params_list: list[dict]) -> IOContent:
        """ Call the client's method multiple times using the given parameters and aggregate results. """
        logger.debug(f"Fetching data from client: {client_obj.__class__.__name__}, method: {method_name}, params: {params_list}")
        method = getattr(client_obj, method_name, None)
        if method is None:
            logger.error(f"Method {method_name} not found in the class {client_obj.__class__.__name__}.")
            raise ValueError(f"Method {method_name} not found in the class {client_obj.__class__.__name__}.")

        outputs = None
        for params in params_list:
            output = method(**params)
            outputs = concat_io_content(outputs, output)

        return outputs

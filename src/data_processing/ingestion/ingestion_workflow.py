# TODO - This script can be improved, it's not very clean and has some redundant code.
from pathlib import Path

import pandas as pd

from src.data_processing import logger
from src.data_processing.utils import order_models, inject_static_fields
from src.data_processing.file_io import FileHandlerFactory
from src.clients import ESPNApiClient, FootballDataApiClient, LiveSoccerScraper, FootballRankingScraper


INSTRUCTIONS_FOLDER_PATH = Path(__file__).parent / "instructions"

# TODO - Add check for the instructions file
# TODO - Potentially separate this script into multiple files
# TODO - Add parallel processing for the ingestion (query each API/website in parallel)
# TODO - Reunite the instructions files into one file + add a key to identify each ingestion instruction => to handle better the manual trigger
# TODO - Add a version control (only process the new data) !!!

INGESTION_CLIENT_REGISTRY = {
    "ESPNApiClient": ESPNApiClient,
    "FootballDataApiClient": FootballDataApiClient,
    "LiveSoccerScraper": LiveSoccerScraper,
    "FootballRankingScraper": FootballRankingScraper
}

def fetch_data_from_client(client_obj: any, method_name: str, params_list: list[dict]) -> list[dict]|pd.DataFrame|None:
    """ TODO """
    logger.debug(f"Fetching data from client: {client_obj.__class__.__name__}, method: {method_name}, params: {params_list}")
    method = getattr(client_obj, method_name, None)
    logger.debug(f"Method found: {method}")
    if method is None:
        raise ValueError(f"Method {method_name} not found in the class {client_obj.__class__.__name__}.")

    outputs = None
    for params in params_list:
        output = method(**params)
        outputs = _append_data(outputs, output)

    logger.debug(f"Outputs: {outputs.head(5) if isinstance(outputs, pd.DataFrame) else outputs[:5]}")
    return outputs

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

def ingestion_workflow(db_repo: str, instructions: dict, manual: bool = False):
    """ TODO """
    logger.info("Starting ingestion workflow.")
    db_repo = Path(db_repo)

    models = instructions.get("models", [])
    models = order_models(models, "landing")
    for model in models:
        model_trigger = model.get("trigger", "automatic")
        if not manual and model_trigger == "manual":
            continue
        if model_trigger != "automatic":
            logger.warning(f"Unsupported trigger type: {model_trigger}.")
            continue

        model_name = model.get("name")
        logger.info(f"Ingestion for model: {model_name}")

        sources = model.get("sources", [])
        if len(sources) != 1:
            logger.error(f"Only one source is supported for ingestion.")
            raise ValueError(f"Only one source is supported for ingestion.")

        client_class = INGESTION_CLIENT_REGISTRY.get(sources[0].get("client_class"))
        if not client_class:
            logger.error(f"Unsupported client class: {sources[0].get('client_class')}.")
            raise ValueError(f"Unsupported client class: {sources[0].get('client_class')}.")

        list_params = model.get("params", [{}])
        method = model.get("method")
        if not method:
            logger.error(f"Method not found in the instruction file.")
            raise ValueError(f"Method not found in the instruction file.")

        client_obj = client_class()
        outputs = fetch_data_from_client(client_obj, method, list_params)

        static_fields = model.get("static_fields", [])
        outputs = inject_static_fields(outputs, static_fields)

        output_file = db_repo / model.get("output", {}).get("path")
        FileHandlerFactory.create_file_handler(output_file).write(outputs, overwrite=False)

    logger.info("Ingestion workflow completed.")

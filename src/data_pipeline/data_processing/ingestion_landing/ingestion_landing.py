from pathlib import Path

import pandas as pd

from . import logger
from src.data_pipeline.data_processing.utils import (
    order_models,
    inject_static_fields
)
from src.data_pipeline.file_io import FileHandlerFactory
from src.clients import (
    ESPNApiClient,
    FootballDataApiClient,
    LiveSoccerScraper,
    FootballRankingScraper
)


CLIENT_REGISTRY = {
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

def ingestion_landing(db_repo: str, instructions: dict, manual: bool = False):
    """ TODO """
    logger.info("Starting ingestion workflow.")
    db_repo = Path(db_repo)

    models = instructions.get("models", [])
    models = order_models(models, "landing")
    for model in models:
        # Trigger
        model_trigger = model.get("trigger", "automatic")
        if model_trigger not in ["automatic", "manual"]:
            logger.warning(f"Unsupported trigger type: {model_trigger}.")
            continue
        elif not manual and model_trigger == "manual":
            continue

        # Name
        model_name = model.get("name")
        logger.info(f"Running ingestion for model: {model_name}")

        # Output handler
        output = model.get("output", {})
        output_handler = FileHandlerFactory.create_file_handler(db_repo / output.get("path"))

        # Processing
        processing = model.get("processing", {})
        client_class = processing.get("client_class")
        method = processing.get("method")
        if not client_class or not method:
            logger.error(f"Client class or method not found in the instruction file.")
            raise ValueError(f"Client class or method not found in the instruction file.")
        processor = CLIENT_REGISTRY.get(client_class)
        if not processor:
            logger.error(f"Unsupported client class: {client_class}.")
            raise ValueError(f"Unsupported client class: {client_class}.")
        list_params = processing.get("params", [{}])
        logger.debug(f"List of parameters: {list_params}")
        outputs = fetch_data_from_client(processor(), method, list_params)

        # Inject static fields
        static_fields = model.get("static_fields", {})
        if static_fields:
            outputs = inject_static_fields(outputs, static_fields)

        # Write to output
        output_handler.write(outputs, overwrite=False)
        logger.info(f"Output written to: {output_handler.path}")

    logger.info("Ingestion workflow completed.")

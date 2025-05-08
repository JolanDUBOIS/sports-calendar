# TODO - This script can be improved, it's not very clean and has some redundant code.
import json
from pathlib import Path
from datetime import datetime
from abc import ABC, abstractmethod

import pandas as pd

from src.data_processing import logger
from src.data_processing.utils import order_models
from src.clients import ESPNApiClient, FootballDataApiClient, LiveSoccerScraper, FootballRankingScraper


INSTRUCTIONS_FOLDER_PATH = Path(__file__).parent / "instructions"

# TODO - Add check for the instructions file
# TODO - Potentially separate this script into multiple files
# TODO - Add parallel processing for the ingestion (query each API/website in parallel)
# TODO - Reunite the instructions files into one file + add a key to identify each ingestion instruction => to handle better the manual trigger
# TODO - Add a version control (only process the new data) !!!

class OutputHandler(ABC):
    """ Abstract base class for handling output. """
    def __init__(self, file_path: Path):
        self.file_path = file_path

    @abstractmethod
    def check_file(self, file_path: Path):
        pass

    @abstractmethod
    def save(self, data, file_path: Path):
        pass

class CSVOutputHandler(OutputHandler):
    """ Handles saving data to CSV files. """
    def __init__(self, file_path: Path):
        super().__init__(file_path)
        self.check_file(file_path)
    
    def check_file(self, file_path: Path):
        if not file_path.suffix == ".csv":
            logger.error(f"File path {file_path} is not a CSV file.")
            raise ValueError(f"File path {file_path} is not a CSV file.")

    def save(self, data: list[pd.DataFrame]):
        if not all(isinstance(df, pd.DataFrame) for df in data):
            logger.error("All elements in the list must be pandas DataFrames.")
            raise ValueError("All elements in the list must be pandas DataFrames.")
        
        logger.debug(f"Data before concatenation: {data}")
        df = pd.concat(data, ignore_index=True)
        df["created_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        if not self.file_path.exists():
            df.to_csv(self.file_path, mode='w', header=True, index=False)
        else:
            df.to_csv(self.file_path, mode='a', header=False, index=False)
        
        logger.info(f"Data appended to {self.file_path}")

class JSONOutputHandler(OutputHandler):
    """ Handles saving data to JSON files. """
    def __init__(self, file_path: Path):
        super().__init__(file_path)
        self.check_file(file_path)

    def check_file(self, file_path: Path):
        if not file_path.suffix == ".json":
            logger.error(f"File path {file_path} is not a JSON file.")
            raise ValueError(f"File path {file_path} is not a JSON file.")

    def save(self, data: list[list[dict]]):
        flattened_data = [item for sublist in data for item in sublist if isinstance(sublist, list) if isinstance(item, dict)]
        for d in flattened_data:
            d["created_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        if not self.file_path.exists():
            with self.file_path.open(mode='w') as file:
                json.dump(flattened_data, file, indent=4)
        else:
            with self.file_path.open(mode='r') as file:
                existing_data = json.load(file)
            existing_data.extend(flattened_data)
            with self.file_path.open(mode='w') as file:
                json.dump(existing_data, file, indent=4)
        
        logger.info(f"Data appended to {self.file_path}")

class OutputHandlerFactory:
    """ Factory to create output handlers based on output type. """
    @staticmethod
    def get_handler(output_file: Path) -> OutputHandler:
        if output_file.suffix == ".csv":
            return CSVOutputHandler(output_file)
        elif output_file.suffix == ".json":
            return JSONOutputHandler(output_file)
        else:
            logger.error(f"Output file {output_file} not supported.")
            raise ValueError(f"Output file {output_file} not supported.")


INGESTION_CLIENT_REGISTRY = {
    "ESPNApiClient": ESPNApiClient,
    "FootballDataApiClient": FootballDataApiClient,
    "LiveSoccerScraper": LiveSoccerScraper,
    "FootballRankingScraper": FootballRankingScraper
}


# def apply_instruction(instruction: dict, client_obj: any) -> list[any]:
#     """ TODO """
#     method_name = instruction.get("method")
#     if method_name is None:
#         raise ValueError("Method not found in the instruction file.")
#     method = getattr(client_obj, method_name, None)
#     if method is None:
#         raise ValueError(f"Method {method_name} not found in the class {client_obj.__class__.__name__}.")

#     outputs = []
#     params_list = instruction.get("params", [{}])
#     for params in params_list:
#         output = method(**params)
#         outputs.append(output)

#     return outputs

def fetch_data_from_client(client_obj: any, method_name: str, params_list: list[dict]) -> list[any]:
    """ TODO """
    logger.debug(f"Fetching data from client: {client_obj.__class__.__name__}, method: {method_name}, params: {params_list}")
    method = getattr(client_obj, method_name, None)
    logger.debug(f"Method found: {method}")
    if method is None:
        raise ValueError(f"Method {method_name} not found in the class {client_obj.__class__.__name__}.")

    outputs = []
    for params in params_list:
        output = method(**params)
        logger.debug(f"Output: {output}")
        outputs.append(output)

    return outputs

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

        output_file = db_repo / model.get("output")
        handler = OutputHandlerFactory.get_handler(output_file)
        handler.save(outputs)
    logger.info("Ingestion workflow completed.")

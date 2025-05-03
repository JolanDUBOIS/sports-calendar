# TODO - This script can be improved, it's not very clean and has some redundant code.
import json
import importlib
from pathlib import Path
from datetime import datetime
from abc import ABC, abstractmethod

import pandas as pd

from src.data_processing import logger
from src.data_processing.utils import read_yml_file


INSTRUCTIONS_FOLDER_PATH = Path(__file__).parent / "instructions"

# TODO - Add check for the instructions file
# TODO - Potentially separate this script into multiple files
# TODO - Add parallel processing for the ingestion (query each API/website in parallel)

class OutputHandler(ABC):
    """ Abstract base class for handling output. """
    @abstractmethod
    def save(self, data, file_path: Path):
        pass

class CSVOutputHandler(OutputHandler):
    """ Handles saving data to CSV files. """
    def save(self, data, file_path: Path):
        if not all(isinstance(df, pd.DataFrame) for df in data):
            logger.error("All elements in the list must be pandas DataFrames.")
            raise ValueError("All elements in the list must be pandas DataFrames.")
        
        df = pd.concat(data, ignore_index=True)
        df["created_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        if not file_path.suffix == ".csv":
            logger.error(f"File path {file_path} is not a CSV file.")
            raise ValueError(f"File path {file_path} is not a CSV file.")
        
        if not file_path.exists():
            df.to_csv(file_path, mode='w', header=True, index=False)
        else:
            df.to_csv(file_path, mode='a', header=False, index=False)
        
        logger.info(f"Data appended to {file_path}")

class JSONOutputHandler(OutputHandler):
    """ Handles saving data to JSON files. """
    def save(self, data, file_path: Path):
        if not file_path.suffix == ".json":
            logger.error(f"File path {file_path} is not a JSON file.")
            raise ValueError(f"File path {file_path} is not a JSON file.")
        
        flattened_data = [item for sublist in data for item in sublist if isinstance(sublist, list) if isinstance(item, dict)]
        for d in flattened_data:
            d["created_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        if not file_path.exists():
            with file_path.open(mode='w') as file:
                json.dump(flattened_data, file, indent=4)
        else:
            with file_path.open(mode='r') as file:
                existing_data = json.load(file)
            existing_data.extend(flattened_data)
            with file_path.open(mode='w') as file:
                json.dump(existing_data, file, indent=4)
        
        logger.info(f"Data appended to {file_path}")

class OutputHandlerFactory:
    """ Factory to create output handlers based on output type. """
    @staticmethod
    def get_handler(output_type: str) -> OutputHandler:
        if output_type == "csv":
            return CSVOutputHandler()
        elif output_type == "json":
            return JSONOutputHandler()
        else:
            logger.error(f"Output type {output_type} not supported.")
            raise ValueError(f"Output type {output_type} not supported.")

def apply_instruction(instruction: dict, client_obj: any) -> list[any]:
    """ TODO """
    method_name = instruction.get("method")
    if method_name is None:
        raise ValueError("Method not found in the instruction file.")
    method = getattr(client_obj, method_name, None)
    if method is None:
        raise ValueError(f"Method {method_name} not found in the class {client_obj.__class__.__name__}.")

    outputs = []
    params_list = instruction.get("params", [{}])
    for params in params_list:
        output = method(**params)
        outputs.append(output)

    return outputs

def execute_ingestion_plan(folder_path: Path, instruction_file: Path, manual: bool = False):
    """ Execute the ingestion plan defined in the instruction file. """
    logger.info(f"Executing ingestion plan from {instruction_file.name}")
    instructions = read_yml_file(instruction_file)
    trigger = instructions.get("trigger", "automatic")
    if not manual and trigger == "manual" or trigger != "automatic":
        logger.info(f"Skipping instructions {instruction_file.name} due to trigger condition.")
        return
    class_name = instructions.get("class_object")
    if class_name is None:
        logger.error("class_object not found in the instruction file.")
        raise ValueError("class_object not found in the instruction file.")
    client_module = importlib.import_module('src.clients')
    client_class = getattr(client_module, class_name, None)
    if client_class is None:
        logger.error(f"Class {class_name} not found in the module src.clients.")
        raise ValueError(f"Class {class_name} not found in the module src.clients.")
    client_obj = client_class()
    if client_obj is None:
        logger.error(f"Class {class_name} not found. Please import it.")
        raise ValueError(f"Class {class_name} not found. Please import it.")

    for instruction in instructions.get("instructions", []):
        outputs = apply_instruction(instruction, client_obj)
        output_type = instruction.get("output_type")
        file_path = folder_path / instruction.get("save_file")
        handler = OutputHandlerFactory.get_handler(output_type)
        handler.save(outputs, file_path)

def ingestion_workflow(db_repo: str, manual: bool = False):
    """ Main function to execute the ingestion workflow. """
    logger.info("Starting ingestion workflow.")
    instruction_files = INSTRUCTIONS_FOLDER_PATH.glob("*.yml")
    landing_path = Path(db_repo) / "landing"
    if not landing_path.exists():
        landing_path.mkdir(parents=True)
        logger.info(f"Created landing folder at {landing_path}")
    for instruction_file in instruction_files:
        execute_ingestion_plan(landing_path, instruction_file, manual)
    logger.info("Ingestion workflow completed.")


if __name__ == "__main__":
    from src.config import get_config
    logger.info("Launching ingestion workflow.")
    ingestion_workflow(get_config("db.path"))
    logger.info("Ingestion workflow executed successfully.")

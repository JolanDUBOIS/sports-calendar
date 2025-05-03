import json
import importlib
from pathlib import Path

import yaml
import pandas as pd

from src.ingestion import logger


INSTRUCTIONS_FOLDER_PATH = Path(__file__).parent / "instructions"

# TODO - Add check for the instructions file

def read_yml_file(file_path: Path):
    """ Read a YAML file and return its content as a dictionary. """
    with file_path.open(mode='r') as file:
        config = yaml.safe_load(file)
    return config

def apply_instruction(instruction: dict, client_obj: any) -> list[any]:
    """ TODO """
    method_name = instruction.get("method")
    if method_name is None:
        raise ValueError("Method not found in the instruction file.")
    method = getattr(client_obj, method_name, None)
    if method is None:
        raise ValueError(f"Method {method_name} not found in the class {client_obj.__class__.__name__}.")

    outputs = []
    params_list = instruction.get("params", [])
    for params in params_list:
        output = method(**params)
        outputs.append(output)

    return outputs

def append_to_csv(df: pd.DataFrame, file_path: Path):
    """ Append a DataFrame to a CSV file. """
    if not file_path.suffix == ".csv":
        logger.error(f"File path {file_path} is not a CSV file.")
        raise ValueError(f"File path {file_path} is not a CSV file.")
    
    if not file_path.exists():
        df.to_csv(file_path, mode='w', header=True, index=False)
    else:
        df.to_csv(file_path, mode='a', header=False, index=False)

    logger.info(f"Data appended to {file_path}")

def save_to_csv(dfs: list[pd.DataFrame], file_path: Path):
    """ Save a list of DataFrames to a CSV file. """
    if not all(isinstance(df, pd.DataFrame) for df in dfs):
        logger.error("All elements in the list must be pandas DataFrames.")
        raise ValueError("All elements in the list must be pandas DataFrames.")
    
    df = pd.concat(dfs, ignore_index=True)
    append_to_csv(df, file_path)

def append_to_json(dicts: list[dict], file_path: Path):
    """ Append a list of dictionaries to a JSON file. """
    if not file_path.suffix == ".json":
        logger.error(f"File path {file_path} is not a JSON file.")
        raise ValueError(f"File path {file_path} is not a JSON file.")
    
    if not file_path.exists():
        with file_path.open(mode='w') as file:
            json.dump(dicts, file, indent=4)
    else:
        with file_path.open(mode='r') as file:
            existing_data = json.load(file)
        existing_data.extend(dicts)
        with file_path.open(mode='w') as file:
            json.dump(existing_data, file, indent=4)
    
    logger.info(f"Data appended to {file_path}")

def save_to_json(dicts: list[dict], file_path: Path):
    """ Save a list of dictionaries to a JSON file. """
    if not all(isinstance(d, dict) for d in dicts):
        logger.error("All elements in the list must be dictionaries.")
        raise ValueError("All elements in the list must be dictionaries.")

    append_to_json(dicts, file_path)

def execute_ingestion_plan(folder_path: Path, instruction_file: Path):
    """ Execute the ingestion plan defined in the instruction file. """
    logger.info(f"Executing ingestion plan from {instruction_file.name}")
    instructions = read_yml_file(instruction_file)
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
        if output_type == "csv":
            save_to_csv(outputs, file_path)
        elif output_type == "json":
            save_to_json(outputs, file_path)
        else:
            logger.error(f"Output type {output_type} not supported.")
            raise ValueError(f"Output type {output_type} not supported.")

def ingestion_workflow(db_repo: str):
    """ Main function to execute the ingestion workflow. """
    logger.info("Starting ingestion workflow.")
    instruction_files = INSTRUCTIONS_FOLDER_PATH.glob("*.yml")
    landing_path = Path(db_repo) / "landing"
    if not landing_path.exists():
        landing_path.mkdir(parents=True)
        logger.info(f"Created landing folder at {landing_path}")
    for instruction_file in instruction_files:
        execute_ingestion_plan(landing_path, instruction_file)
    logger.info("Ingestion workflow completed.")


if __name__ == "__main__":
    from src.config import get_config
    logger.info("Launching ingestion workflow.")
    ingestion_workflow(get_config("db.path"))
    logger.info("Ingestion workflow executed successfully.")

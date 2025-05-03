from pathlib import Path
import json

import pandas as pd

from .utils import extract_from_json
from src.data_processing import logger
from src.data_processing.utils import read_yml_file


INSTRUCTIONS_FILE_PATH = Path(__file__).parent / "instructions" / "data_extraction.yml"

def read_source(file_path: Path, source_type: str) -> list[dict]:
    """Read data from a specified source type."""
    if source_type == "json":
        with file_path.open(mode='r') as file:
            return json.load(file)
    else:
        logger.error(f"Unsupported source type: {source_type}.")
        raise ValueError(f"Unsupported source type: {source_type}.")

def save_file(file_path: Path, data: list[dict]):
    """Save data to a CSV file."""
    pd.DataFrame(data).to_csv(file_path, index=False)

def extraction_workflow(db_repo: str):
    """Run the extraction workflow."""
    logger.info("Starting extraction workflow...")
    instructions_dict = read_yml_file(INSTRUCTIONS_FILE_PATH)
    intermediate_path = Path(db_repo) / "intermediate"
    if not intermediate_path.exists():
        intermediate_path.mkdir(parents=True)
        logger.info(f"Created intermediate folder at {intermediate_path}")
    for _, instructions in instructions_dict.items():
        landing_file_path = instructions.get("landing_file")
        extraction_file_path = instructions.get("extraction_file")
        extraction_instructions = instructions.get("columns", {})
        source_type = instructions.get("source_type", "json")  # Default to JSON
        source_data = read_source(landing_file_path, source_type)
        if source_type == "json":
            extracted_data = extract_from_json(source_data, extraction_instructions)
        else:
            logger.error(f"Extraction not implemented for source type: {source_type}.")
            raise NotImplementedError(f"Extraction not implemented for source type: {source_type}.")
        save_file(intermediate_path / extraction_file_path, extracted_data)
    logger.info("Extraction workflow completed.")

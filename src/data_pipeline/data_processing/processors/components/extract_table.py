from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path

import yaml
import pandas as pd

from . import logger


# Specifications for extraction instructions

@dataclass
class ExtractionSpec:
    extraction_type: str
    columns_mapping: dict[str, str | list[str]]

    def __post_init__(self):
        valid_extraction_types = {"simple", "double"}
        if self.extraction_type not in valid_extraction_types:
            logger.error(f"Invalid extraction type: {self.extraction_type}. Expected one of {valid_extraction_types}.")
            raise ValueError(f"Invalid extraction type: {self.extraction_type}. Expected one of {valid_extraction_types}.")
        if self.extraction_type == "simple" and not all(isinstance(source, str) for source in self.columns_mapping.values()):
            logger.error("For 'simple' extraction type, all source fields must be strings.")
            raise TypeError("For 'simple' extraction type, all source fields must be strings.")
        if self.extraction_type == "double":
            if not all(isinstance(source, list) and len(source) == 2 for source in self.columns_mapping.values()):
                logger.error("For 'double' extraction type, all source fields must be lists of exactly two strings.")
                raise TypeError("For 'double' extraction type, all source fields must be lists of exactly two strings.")

    def is_simple(self) -> bool:
        """ Checks if the extraction specification is simple """
        return self.extraction_type == "simple"

    @classmethod
    def from_dict(cls, d: dict) -> ExtractionSpec:
        """ Creates an ExtractionSpec from a dictionary. """
        return cls(
            extraction_type=d["extraction_type"],
            columns_mapping=d["columns_mapping"]
        )

def load_extraction_specs_from_yml(path: str | Path) -> dict[str, ExtractionSpec]:
    path = Path(path)
    with path.open(mode='r') as file:
        raw_data = yaml.safe_load(file)
    
    if not isinstance(raw_data, dict):
        logger.error(f"Invalid YAML format in {path}. Expected a dictionary.")
        raise TypeError(f"Invalid YAML format in {path}. Expected a dictionary.")

    extraction_specs = {}
    for key, value in raw_data.items():
        if not isinstance(value, dict):
            logger.error(f"Invalid format for extraction spec '{key}'. Expected a dictionary.")
            raise TypeError(f"Invalid format for extraction spec '{key}'. Expected a dictionary.")
        extraction_specs[key] = ExtractionSpec.from_dict(value)

    return extraction_specs

EXTRACTION_SPECIFICATIONS = load_extraction_specs_from_yml(Path(__file__).parent / "config" / "extract_table.yml")

# Table extraction functions

def extract_table(data: pd.DataFrame, key: str, **kwargs) -> pd.DataFrame:
    """ Extracts data from a DataFrame based on predefined instructions for the given key. """
    if not isinstance(data, pd.DataFrame):
        logger.error("The source should be a pandas DataFrame.")
        raise ValueError("The source should be a pandas DataFrame.")
    specs = EXTRACTION_SPECIFICATIONS.get(key)
    if specs is None:
        logger.error(f"No extraction specifications found for key: {key}")
        raise ValueError(f"No extraction specifications found for key: {key}")
    if specs.is_simple():
        return _simple_extraction(data, specs.columns_mapping, **kwargs)
    else:
        return _double_extraction(data, specs.columns_mapping, **kwargs)

def _simple_extraction(
    data: pd.DataFrame,
    col_mapping: list[str],
    deduplicate: bool = False
) -> pd.DataFrame:
    """ Extract specified columns from the DataFrame, optionally deduplicating. """
    _df = data[list(col_mapping)].rename(columns=col_mapping)
    if deduplicate:
        logger.debug("Deduplicating data")
        _df = _df.drop_duplicates()
    return _df

def _double_extraction(
    data: pd.DataFrame,
    col_mapping: dict[str, list[str]],
    deduplicate: bool = False
) -> pd.DataFrame:
    """
    Extracts and renames columns from the DataFrame using a columns mapping.
    Each key in columns_mapping is the target column name, and the value is a list of source column names.
    """
    target_columns = col_mapping.keys()
    output_data = pd.DataFrame(columns=target_columns)
    for columns in zip(*col_mapping.values()):
        logger.debug(f"Extracting columns: {columns}")
        if not all(col in data.columns for col in columns):
            logger.error(f"One of the columns {columns} is not present in the data.")
            raise ValueError(f"One of the columns {columns} is not present in the data.")
        new_rows = data[list(columns)].rename(columns=dict(zip(columns, target_columns)))
        output_data = pd.concat([output_data, new_rows], ignore_index=True)
    if deduplicate:
        logger.debug("Deduplicating data")
        output_data = output_data.drop_duplicates()
    return output_data

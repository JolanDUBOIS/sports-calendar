from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path

import yaml
import pandas as pd

from . import logger


# Specifications for columns mapping

class JsonPath:
    """ Represents a path to a JSON field. """

    def __init__(self, path: str):
        self.path = path
        self._keys = None

    @property
    def keys(self) -> list[str | int]:
        """ Returns the keys in the path as a list. """
        if self._keys is None:
            self._keys = [int(key) if key.isdigit() else key for key in self.path.split(".")]
        return self._keys

    def apply(self, data: dict) -> any:
        """ Applies the path to the given data and returns the value. """
        for key in self.keys:
            try:
                data = data[key]
            except (KeyError, IndexError, TypeError):
                logger.debug(f"Key '{key}' not found in data.")
                return None
        return data

@dataclass(frozen=True)
class Column:
    name: str
    path: JsonPath

@dataclass(frozen=True)
class Columns:
    cols: list[Column]

    def __iter__(self):
        return iter(self.cols)

    def __len__(self):
        return len(self.cols)

    def get(self, name: str) -> Column | None:
        """ Returns the column with the given name, or None if not found. """
        for col in self.cols:
            if col.name == name:
                return col
        return None

    def apply(self, data: dict) -> dict:
        """ Applies the columns to the given data and returns a dictionary of column names to values. """
        result = {}
        for col in self.cols:
            result[col.name] = col.path.apply(data)
        return result

    @classmethod
    def from_dict(cls, d: dict[str, str]) -> Columns:
        """ Creates a Columns instance from a dictionary mapping column names to JSON paths. """
        cols = [Column(name, JsonPath(path)) for name, path in d.items()]
        return cls(cols)

@dataclass
class IterateMapping:
    path: JsonPath
    columns: Columns

    def apply(self, data: dict) -> list[dict]:
        """ Applies the iterate mapping to the given data and returns a list of dictionaries. """
        iteration_list = self.path.apply(data)
        if not isinstance(iteration_list, list):
            logger.error(f"Expected a list at path '{self.path.path}', but got {type(iteration_list).__name__}.")
            raise ValueError(f"Expected a list at path '{self.path.path}', but got {type(iteration_list).__name__}.")

        extracted_data = []
        for item in iteration_list:
            extracted_data.append(self.columns.apply(item))

        logger.debug(f"Extracted {len(extracted_data)} items from path '{self.path.path}'.")
        return extracted_data

    @classmethod
    def from_dict(cls, d: dict) -> IterateMapping:
        """ Creates an IterateMapping instance from a dictionary mapping column names to JSON paths. """
        path = JsonPath(d["path"])
        columns = Columns.from_dict(d["columns"])
        return cls(path, columns)

@dataclass
class ColumnsMapping:
    direct_paths: Columns
    iterate: IterateMapping | None = None

    def apply(self, data: dict) -> list[dict]:
        """ Applies the columns mapping to the given data and returns a list of dictionaries. """
        extracted_data = self.direct_paths.apply(data)
        if self.iterate:
            iterated_data = self.iterate.apply(data)
            for item in iterated_data:
                item.update(extracted_data)
            return iterated_data
        else:
            return [extracted_data]

    @classmethod
    def from_dict(cls, d: dict) -> ColumnsMapping:
        """ Creates a ColumnsMapping instance from a dictionary. """
        direct_paths = Columns.from_dict(d["direct_paths"])
        iterate = IterateMapping.from_dict(d["iterate"]) if "iterate" in d else None
        return cls(direct_paths, iterate)


def load_columns_mapping_from_yaml(path: str | Path) -> dict[str, ColumnsMapping]:
    path = Path(path)
    with path.open(mode='r') as file:
        raw_data = yaml.safe_load(file)

    if not isinstance(raw_data, dict):
        logger.error("Invalid format for columns mapping. Expected a dictionary.")
        raise ValueError("Invalid format for columns mapping. Expected a dictionary.")

    columns_mapping = {}
    for key, value in raw_data.items():
        if not isinstance(value, dict):
            logger.error(f"Invalid format for columns mapping for key '{key}'. Expected a dictionary.")
            raise ValueError(f"Invalid format for columns mapping for key '{key}'. Expected a dictionary.")
        columns_mapping[key] = ColumnsMapping.from_dict(value)

    return columns_mapping

COLUMNS_MAPPING_SPECS = load_columns_mapping_from_yaml(Path(__file__).parent / "config" / "columns_mapping.yml")

# JSON extraction functions

def extract_json(data: list[dict], output_key: str, **kwargs) -> pd.DataFrame:
    """ Extracts data from JSON files. """
    mapping = COLUMNS_MAPPING_SPECS.get(output_key)
    logger.debug(f"Extracting data with mapping: {mapping}")

    extracted_data = []
    for item in data:
        extracted_data.extend(mapping.apply(item))
        logger.debug(f"Extracted {len(extracted_data)} items so far.")

    return pd.DataFrame(extracted_data)

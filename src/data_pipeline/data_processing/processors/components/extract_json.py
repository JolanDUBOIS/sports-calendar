from __future__ import annotations
from dataclasses import dataclass

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


# JSON extraction functions

def extract_json(data: list[dict], columns_mapping: dict, **kwargs) -> pd.DataFrame:
    """ Extracts data from JSON files. """
    mapping = ColumnsMapping.from_dict(columns_mapping)
    logger.debug(f"Extracting data with mapping: {mapping}")

    extracted_data = []
    for item in data:
        extracted_data.extend(mapping.apply(item))
        logger.debug(f"Extracted {len(extracted_data)} items so far.")

    return pd.DataFrame(extracted_data)

# def extract_json(data: list[dict], columns_mapping: dict, **kwargs) -> pd.DataFrame:
#     """ Extracts data from JSON files. """
#     extracted_data = []
#     for item in data:
#         # logger.debug(f"Extracting item: {item} with mapping: {columns_mapping}")
#         extracted_item = _extract_item(item, columns_mapping)
#         extracted_data.extend(extracted_item)

#     data = pd.DataFrame(extracted_data)
#     logger.debug(f"Number of rows after extraction: {len(data)}")
#     return data

# def _extract_item(json_item: dict, columns_mapping: dict) -> list[dict]:
#     """ Extracts data from a JSON item according to the columns mapping. """
#     # Iterate over the 'direct_paths' columns
#     direct_data = {}
#     for col_name, path in columns_mapping.get("direct_paths", {}).items():
#         direct_data[col_name] = _extract(json_item, path)

#     # Iterate over the 'iterate' columns
#     iterated_mapping = columns_mapping.get("iterate")
#     if not iterated_mapping:
#         return [direct_data]

#     path = iterated_mapping.get('path')
#     columns = iterated_mapping.get('columns', {})
#     list_dict = _extract(json_item, path)
#     extracted_data = []
#     for item in list_dict:
#         item_data = direct_data.copy()
#         for col_name, col_path in columns.items():
#             item_data[col_name] = _extract(item, col_path)
#         extracted_data.append(item_data)

#     return extracted_data

# def _extract(d: dict, path: str) -> any:
#     """ Extracts a value from a nested dictionary using a dot-separated path. """
#     keys = path.split(".")
#     for key in keys:
#         try:
#             key = int(key)
#         except ValueError:
#             pass
#         try:
#             d = d[key]
#         except (KeyError, IndexError):
#             # logger.debug(f"Key '{key}' not found in dictionary.")
#             return None
#     return d

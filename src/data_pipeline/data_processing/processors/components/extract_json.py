import pandas as pd

from . import logger


def extract_json(data: list[dict], columns_mapping: dict, **kwargs) -> pd.DataFrame:
    """ Extracts data from JSON files. """
    extracted_data = []
    for item in data:
        logger.debug(f"Extracting item: {item} with mapping: {columns_mapping}")
        extracted_item = _extract_item(item, columns_mapping)
        extracted_data.append(extracted_item)

    return pd.DataFrame(extracted_data)

def _extract_item(json_item: dict, columns_mapping: dict) -> dict:
    """ TODO """
    extracted_data = {}

    # Iterate over the 'paths' columns
    for col_name, path in columns_mapping.get("direct_paths", {}).items():
        logger.debug(f"Extracting value for column: {col_name} from path: {path}")
        extracted_data[col_name] = _extract(json_item, path)

    # Iterate over the 'iterate' columns
    iterated_mapping = columns_mapping.get("iterate")
    if not iterated_mapping:
        logger.debug("No iterated mapping found.")
        return extracted_data

    path = iterated_mapping.get('path')
    columns = iterated_mapping.get('columns', [])
    logger.debug(f"Iterating over path: {path} for columns: {columns}")
    list_dict = _extract(json_item, path)
    for item in list_dict:
        for col_name in columns:
            extracted_data[col_name] = _extract(item, col_name)

    return extracted_data

def _extract(d: dict, path: str) -> any:
    """ Extracts a value from a nested dictionary using a dot-separated path. """
    logger.debug(f"Extracting value from path: {path}")
    keys = path.split(".")
    for key in keys:
        try:
            key = int(key)
        except ValueError:
            pass
        try:
            d = d[key]
        except (KeyError, IndexError):
            logger.debug(f"Key {key} not found in dictionary.")
            return None
    return d

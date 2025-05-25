import pandas as pd

from . import logger


def extract_json(data: list[dict], columns_mapping: dict, **kwargs) -> pd.DataFrame:
    """ Extracts data from JSON files. """
    extracted_data = []
    for item in data:
        # logger.debug(f"Extracting item: {item} with mapping: {columns_mapping}")
        extracted_item = _extract_item(item, columns_mapping)
        extracted_data.extend(extracted_item)

    data = pd.DataFrame(extracted_data)
    logger.debug(f"Number of rows after extraction: {len(data)}")
    return data

def _extract_item(json_item: dict, columns_mapping: dict) -> list[dict]:
    """ TODO """
    # Iterate over the 'direct_paths' columns
    direct_data = {}
    for col_name, path in columns_mapping.get("direct_paths", {}).items():
        direct_data[col_name] = _extract(json_item, path)

    # Iterate over the 'iterate' columns
    iterated_mapping = columns_mapping.get("iterate")
    if not iterated_mapping:
        return [direct_data]

    path = iterated_mapping.get('path')
    columns = iterated_mapping.get('columns', {})
    list_dict = _extract(json_item, path)
    extracted_data = []
    for item in list_dict:
        item_data = direct_data.copy()
        for col_name, col_path in columns.items():
            item_data[col_name] = _extract(item, col_path)
        extracted_data.append(item_data)

    return extracted_data

def _extract(d: dict, path: str) -> any:
    """ Extracts a value from a nested dictionary using a dot-separated path. """
    keys = path.split(".")
    for key in keys:
        try:
            key = int(key)
        except ValueError:
            pass
        try:
            d = d[key]
        except (KeyError, IndexError):
            # logger.debug(f"Key '{key}' not found in dictionary.")
            return None
    return d

import pandas as pd

from src.data_processing import logger


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

def extract_from_json(
    sources: dict[str, dict],
    columns_mapping: dict
) -> pd.DataFrame:
    """ Extracts data from a list of JSON objects based on the provided instructions. """
    if len(sources) != 1:
        logger.error("Only one source is supported for json extraction.")
        raise ValueError("Only one source is supported for json extraction.")
    json_data = next(iter(sources.values()))
    if not isinstance(json_data, list) and all(isinstance(i, dict) for i in json_data):
        # Maybe too computationally expensive to go through all the elements already
        logger.error("The source should be a list of dictionaries.")
        raise ValueError("The source should be a list of dictionaries.")

    extracted_data = []
    for item in json_data:
        extracted_item = _extract_item(item, columns_mapping)
        extracted_data.append(extracted_item)

    return pd.DataFrame(extracted_data)

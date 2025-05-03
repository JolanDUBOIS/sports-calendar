def _extract(d: dict, path: str) -> any:
    """ Extracts a value from a nested dictionary using a dot-separated path. """
    keys = path.split(".")
    for key in keys:
        try:
            key = int(key)
        except ValueError:
            pass
        d = d[key]
    return d

def _extract_data(json_data: dict, instructions: dict) -> dict:
    """ TODO """
    extracted_data = {}
    
    # Iterate over the 'paths' columns
    for col_name, path in instructions.get("paths", {}).items():
        extracted_data[col_name] = _extract(json_data, path)
    
    # Iterate over the 'injected' columns
    for col_name, value in instructions.get("injected", {}).items():
        extracted_data[col_name] = value

    # Iterate over the 'iterate' columns
    path = instructions.get("iterate", {}).get('path')
    columns = instructions.get("iterate", {}).get('columns', [])
    list_dict = _extract(json_data, path)
    for item in list_dict:
        for col_name in columns:
            extracted_data[col_name] = _extract(item, col_name)
    
    return extracted_data

def extract_from_json(json_data: list[dict], instructions: dict) -> list[dict]:
    """ Extracts data from a list of JSON objects based on the provided instructions. """
    extracted_data = []
    
    for item in json_data:
        extracted_item = _extract_data(item, instructions)
        extracted_data.append(extracted_item)
    
    return extracted_data

from pathlib import Path
from datetime import datetime, timedelta

import yaml


def date_offset_constructor(loader, node):
    """ Custom YAML constructor to handle date offsets. """
    days = int(node.value)
    return (datetime.today() + timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")

def include_constructor(loader, node):
    """ Custom YAML constructor to include part of another YAML file. """
    value = loader.construct_mapping(node)
    file = value["file"]
    model_name = value["model_name"]

    # Resolve path relative to the current YAML file being loaded
    base_path = Path(loader.name).parent
    include_path = base_path / file

    # Load the included YAML content
    with open(include_path, "r") as f:
        included_yaml = yaml.safe_load(f)

    # Extract the corresponding model's column mapping
    for model in included_yaml.get("models", []):
        if model.get("name") == model_name:
            return model.get("columns_mapping", {})

    raise ValueError(f"No model named '{model_name}' found in '{file}'")

yaml.add_constructor('!date_offset', date_offset_constructor, Loader=yaml.loader.SafeLoader)
yaml.add_constructor('!include', include_constructor, Loader=yaml.SafeLoader)


def read_yml_file(file_path: Path) -> dict:
    """ Read a YAML file and return its content as a dictionary. """
    with file_path.open(mode='r') as file:
        config = yaml.safe_load(file)
    return config

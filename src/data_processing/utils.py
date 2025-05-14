import hashlib
from enum import IntEnum
from pathlib import Path
from datetime import datetime, timedelta

import yaml
import pandas as pd

from src.data_processing import logger
from src.data_processing.file_io import FileHandlerFactory


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


def read_yml_file(file_path: Path):
    """ Read a YAML file and return its content as a dictionary. """
    with file_path.open(mode='r') as file:
        config = yaml.safe_load(file)
    return config


class DataStage(IntEnum):
    LANDING = 0
    INTERMEDIATE = 1
    STAGING = 2
    PRODUCTION = 3

    @classmethod
    def from_str(cls, name: str) -> "DataStage":
        try:
            return cls[name.strip().upper()]
        except KeyError:
            raise ValueError(f"Invalid DataStage name: {name}")

def order_models(models: list[dict], target_stage: str) -> list[dict]:
    """ Order models based on their dependencies. """
    target_stage = DataStage.from_str(target_stage)

    model_dependencies = {}
    for model in models:
        model_name = model.get('name')
        model_dep = model.get('dependencies', [])
        model_dependencies[model_name] = []
        for dependency in model_dep:
            dependency_stage = DataStage.from_str(dependency.split('.')[0])
            dependency_name = dependency.split('.')[1]
            if dependency_stage > target_stage:
                logger.error(f"Model {model_name} has a dependency on a model in a later stage: {dependency}")
                raise ValueError(f"Model {model_name} has a dependency on a model in a later stage: {dependency}")
            if dependency_stage == target_stage:
                # Only the dependencies on the same stage could be an issue later and should be considered for ordering
                model_dependencies[model_name].append(dependency_name)

    logger.debug(f"Model dependencies: {model_dependencies}")
    ordered_models = []
    counter = 0
    max_iter = 2 * len(model_dependencies)
    while len(ordered_models) < len(model_dependencies):
        for model_name, dependency_names in list(model_dependencies.items()):
            if not dependency_names:
                logger.debug(f"Model {model_name} has no dependencies, adding to ordered models.")
                ordered_models.append(model_name)
                model_dependencies.pop(model_name)
            elif all(dep in ordered_models for dep in dependency_names):
                logger.debug(f"All dependencies for model {model_name} are satisfied, adding to ordered models.")
                ordered_models.append(model_name)
                model_dependencies.pop(model_name)

        logger.debug(f"Current ordered models: {ordered_models}")
        logger.debug(f"Counter: {counter}")
        counter += 1
        if counter > max_iter:
            logger.error(f"Dependency resolution failed for models: {model_dependencies}")
            raise ValueError(f"Dependency resolution failed for models: {model_dependencies}")

    ordered_models = [
        model
        for ordered_model in ordered_models
        for model in models
        if model.get('name') == ordered_model
    ]
    return ordered_models


def inject_static_fields(data: list[dict]|pd.DataFrame, static_fields: list[dict]) -> list[dict]|pd.DataFrame:
    """ Inject static fields into the data. """
    if isinstance(data, list):
        return _inject_static_fields_json(data, static_fields)
    elif isinstance(data, pd.DataFrame):
        return _inject_static_fields_df(data, static_fields)
    else:
        logger.debug(f"Data type: {type(data)}")
        logger.debug(f"Static fields: {static_fields}")
        logger.error("Data should be either a list of dictionaries or a pandas DataFrame.")
        raise ValueError("Data should be either a list of dictionaries or a pandas DataFrame.")

def _inject_static_fields_json(data: list[dict], static_fields: list[dict]) -> list[dict]:
    """ Inject static fields into a list of dictionaries. """
    for item in data:
        for static_field in static_fields:
            key = static_field.get("name")
            value = static_field.get("value")
            if key not in item:
                item[key] = value
            else:
                logger.warning(f"Static field {key} already exists in the data.")
    return data

def _inject_static_fields_df(data: pd.DataFrame, static_fields: list[dict]) -> pd.DataFrame:
    """ Inject static fields into a DataFrame. """
    for static_field in static_fields:
        key = static_field.get("name")
        value = static_field.get("value")
        if key not in data.columns:
            data[key] = value
        else:
            logger.warning(f"Static field {key} already exists in the data.")
    return data


def generate_hash_id(*fields) -> str:
    return hashlib.sha256('|'.join(str(field) for field in fields).encode('utf-8')).hexdigest()


def get_subclass(class_instance: object, subclass_name: str) -> object|None:
    """ Get subclass of a class instance by name """
    for subclass in class_instance.__subclasses__():
        if subclass.__name__ == subclass_name:
            return subclass
    return None


def load_sources(sources: list[dict], db_repo: Path, version_threshold: any = None) -> dict[str, dict|pd.DataFrame]:
    """ TODO """
    loaded_sources = {}
    for source in sources:
        source_name = source.get("name")
        source_path = source.get("path")
        source_versioning = source.get("versioning", {})
        version_mode = source_versioning.get("mode", "all")
        version_on = source_versioning.get("on", "created_at")
        version_type = source_versioning.get("version_type", "datetime")

        file_handler = FileHandlerFactory.create_file_handler(db_repo / source_path)
        loaded_sources[source_name] = file_handler.read(
            mode=version_mode,
            on=version_on,
            version_threshold=version_threshold,
            version_type=version_type
        )
    
    return loaded_sources


# Schema enforcement

def _check_schema(schema: list[dict]) -> None:
    """ TODO """
    if not isinstance(schema, list):
        logger.debug(f"Schema: {schema}")
        logger.error("Schema must be a list.")
        raise TypeError("Schema must be a list.")
    if not all(isinstance(col, dict) for col in schema):
        logger.debug(f"Schema: {schema}")
        logger.error("All elements in the schema must be dictionaries.")
        raise TypeError("All elements in the schema must be dictionaries.")
    for col in schema:
        if "name" not in col:
            logger.debug(f"Schema: {schema}")
            logger.error("Column name is missing in the schema.")
            raise ValueError("Column name is missing in the schema.")

def enforce_schema(data: pd.DataFrame, schema: list[dict], strict: bool = True) -> pd.DataFrame: # TODO - This shouldn't be here
    """ TODO """
    _check_schema(schema)
    if not isinstance(data, pd.DataFrame):
        logger.error("Data must be a pandas DataFrame.")
        raise TypeError("Data must be a pandas DataFrame.")
    if data.empty:
        logger.debug("Data is empty.")
        return data
    
    for col in schema:

        col_name = col.get("name")
        if col_name not in data.columns:
            if strict:
                logger.error(f"Column {col_name} is missing in the DataFrame.")
                raise ValueError(f"Column {col_name} is missing in the DataFrame.")
            else:
                logger.warning(f"Column {col_name} is missing in the DataFrame. Adding it with None values.")
                data[col_name] = None

        col_type = col.get("type")
        if col_type:
            try:
                data[col_name] = data[col_name].astype(col_type)
            except Exception as e:
                if strict:
                    raise TypeError(f"Failed to cast column {col_name} to {col_type}: {e}")
                else:
                    logger.warning(f"Failed to cast column {col_name} to {col_type}: {e}")

        col_unique = col.get("unique", False)
        if col_unique:
            if strict:
                if data[col_name].duplicated().any():
                    logger.error(f"Column {col_name} has duplicates.")
                    raise ValueError(f"Column {col_name} has duplicates.")
            else:
                col_ordered_by = col.get("ordered_by", "created_at")
                col_ascending = col.get("ascending", False)
                data.sort_values(by=col_ordered_by, ascending=col_ascending, inplace=True)
                data.drop_duplicates(subset=[col_name], keep="first", inplace=True)
                logger.debug(f"Column {col_name} has been made unique by dropping duplicates.")

    return data

import hashlib
from enum import IntEnum
from pathlib import Path

import pandas as pd

from src.data_pipeline.data_processing import logger
from src.data_pipeline.file_io import FileHandlerFactory


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

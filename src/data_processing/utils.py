from enum import IntEnum
from pathlib import Path
from datetime import datetime, timedelta

import yaml

from src.data_processing import logger


def date_offset_constructor(loader, node):
    """ Custom YAML constructor to handle date offsets. """
    days = int(node.value)
    return (datetime.today() + timedelta(days=days)).strftime("%Y-%m-%d")

yaml.add_constructor('!date_offset', date_offset_constructor, Loader=yaml.loader.SafeLoader)

def read_yml_file(file_path: Path):
    """ Read a YAML file and return its content as a dictionary. """
    with file_path.open(mode='r') as file:
        config = yaml.safe_load(file)
    return config


class DataStage(IntEnum):
    LANDING = 0
    INTERMEDIATE = 1
    STAGING = 2
    SERVING = 3

    @classmethod
    def from_str(cls, name: str) -> "DataStage":
        try:
            return cls[name.strip().upper()]
        except KeyError:
            raise ValueError(f"Invalid DataStage name: {name}")

def order_models(models: list[dict], stage: str) -> list[dict]:
    """ Order models based on their dependencies. """
    stage = DataStage.from_str(stage)

    model_dependencies = {}
    for model in models:
        model_name = model.get('name')
        model_dep = model.get('dependencies', [])
        model_dependencies[model_name] = []
        for dependency in model_dep:
            dependency_stage = DataStage.from_str(dependency.split('.')[0])
            dependency_name = dependency.split('.')[1]
            if dependency_stage > stage:
                logger.error(f"Model {model_name} has a dependency on a model in a later stage: {dependency}")
                raise ValueError(f"Model {model_name} has a dependency on a model in a later stage: {dependency}")
            if dependency_stage == stage:
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
    logger.debug(f"Final ordered models: {ordered_models}")
    return ordered_models
                        
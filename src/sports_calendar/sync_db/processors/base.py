from __future__ import annotations
from typing import TYPE_CHECKING, Any
from pathlib import Path
from abc import ABC, abstractmethod

from . import logger
from sports_calendar.core import load_yml

if TYPE_CHECKING:
    from sports_calendar.core import IOContent
    from sports_calendar.sync_db.definitions.specs import ProcessingStepSpec, ProcessingIOInfo


def load_processor_configs(path: Path) -> dict[str, dict[str, Any]]:
    """ Load processors from the specified path. """
    processors_config = {}
    for file in path.glob("*.yml"):
        if not file.is_file():
            logger.warning(f"Skipping non-file entry in processors directory: {file}")
            continue
        config = load_yml(file)
        processors_config[file.stem] = config
    return processors_config

processor_configs = load_processor_configs(Path(__file__).parent / "recipes")
processor_config_overrides: dict[str, dict[str, Any]] = {}  # Override dict for tests / temporary changes

class Processor(ABC):
    """ Base class for all processors. """
    config_filename: str

    @classmethod
    def run(cls, data: dict[str, IOContent], processing_step_spec: ProcessingStepSpec) -> dict[str, IOContent]:
        """ Run the processor with given arguments and return the output DataFrame. """
        logger.info(f"Running processor: {cls.__name__}.")
        logger.debug(f"Running processor with processing step spec: {processing_step_spec}.")
        localized_data = cls.localize_names(data, processing_step_spec.io_info)
        kwargs = processing_step_spec.to_dict(deep=False)
        output_io_content = cls._run(data=localized_data, **kwargs)
        data[processing_step_spec.io_info.output_key] = output_io_content
        return data

    @staticmethod
    def localize_names(data: dict[str, IOContent], io_info: ProcessingIOInfo) -> dict[str, IOContent]:
        """ Localize the names of the input data according to the IO information. """
        localized_data = {}
        for key, value in io_info.input_keys.items():
            if value in data:
                localized_data[key] = data[value]
        return localized_data

    @classmethod
    def load_config(cls, config_key: str) -> dict | list:
        """ Load configuration for the processor. """
        config_data = {**processor_configs, **processor_config_overrides}.get(cls.config_filename, {})
        if config_key not in config_data:
            logger.error(f"Configuration key '{config_key}' not found in processor configuration.")
            raise KeyError(f"Configuration key '{config_key}' not found in processor configuration.")
        return config_data.get(config_key)

    @classmethod
    @abstractmethod
    def _run(cls, **kwargs) -> IOContent:
        """ TODO """
        raise NotImplementedError("Subclasses must implement the _run method.")

from abc import ABC, abstractmethod

import pandas as pd

from . import logger
from .components import drop_na, deduplicate
from ...types import IOContent


class Processor(ABC):
    """ Base class for all processors. """

    def run(self, **kwargs) -> IOContent:
        """ Run the processor. """
        data = self._run(**kwargs)
        return data

    @abstractmethod
    def _run(self, **kwargs) -> IOContent:
        """ TODO """
        pass

    @staticmethod
    def _check_json_data(json_data: list[dict]) -> None:
        """ TODO """
        if not isinstance(json_data, list) and all(isinstance(i, dict) for i in json_data):
            # Maybe too computationally expensive to go through all the elements already
            logger.error("The source should be a list of dictionaries.")
            raise ValueError("The source should be a list of dictionaries.")

    @staticmethod
    def _check_dataframe(data: pd.DataFrame) -> None:
        """ TODO """
        if not isinstance(data, pd.DataFrame):
            logger.error("The source should be a pandas DataFrame.")
            raise ValueError("The source should be a pandas DataFrame.")

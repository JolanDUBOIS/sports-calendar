from abc import ABC, abstractmethod

import pandas as pd

from . import logger


class Processor(ABC):
    """ Base class for all processors. """

    @abstractmethod
    def run(self, *args, **kwargs):
        """ Run the processor. """
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

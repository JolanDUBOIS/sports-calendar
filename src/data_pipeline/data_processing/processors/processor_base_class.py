from __future__ import annotations
from typing import TYPE_CHECKING
from abc import ABC, abstractmethod

import pandas as pd

from . import logger

if TYPE_CHECKING:
    from ...types import IOContent


class Processor(ABC):
    """ Base class for all processors. """

    def run(self, **kwargs) -> IOContent:
        """ Run the processor with given arguments and return the output content. """
        data = self._run(**kwargs)
        return data

    @abstractmethod
    def _run(self, **kwargs) -> IOContent:
        """ Execute the core logic of the processor. Must be implemented by subclasses. """
        pass

    @staticmethod
    def _check_json_data(json_data: list[dict]) -> None:
        """ Check that the input is a list of dictionaries. """
        if not isinstance(json_data, list) and all(isinstance(i, dict) for i in json_data):
            # Maybe too computationally expensive to go through all the elements already
            logger.error("The source should be a list of dictionaries.")
            raise ValueError("The source should be a list of dictionaries.")

    @staticmethod
    def _check_dataframe(data: pd.DataFrame) -> None:
        """ Check that the input is a pandas DataFrame. """
        if not isinstance(data, pd.DataFrame):
            logger.error("The source should be a pandas DataFrame.")
            raise ValueError("The source should be a pandas DataFrame.")

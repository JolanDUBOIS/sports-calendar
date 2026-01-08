from __future__ import annotations
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Type

import pandas as pd

from . import logger
if TYPE_CHECKING:
    from ..filters import SelectionFilter


class FilterApplier(ABC):
    """ Base class for filter appliers. """

    def apply(self, filter: SelectionFilter, df: pd.DataFrame) -> pd.DataFrame:
        """ Dispatches the filter to the correct apply method based on its type. """
        dispatch_map = self._get_dispatch_map()

        for filter_type, handler in dispatch_map.items():
            if isinstance(filter, filter_type):
                return handler(filter, df)

        logger.error(f"No handler defined for filter type: {type(filter).__name__}")
        raise NotImplementedError(f"No handler defined for filter type: {type(filter).__name__}")

    @abstractmethod
    def _get_dispatch_map(self) -> dict[Type[SelectionFilter], callable]:
        """ Returns a mapping of filter types to their respective handler methods. """
        raise NotImplementedError("Subclasses must implement the _get_dispatch_map method.")

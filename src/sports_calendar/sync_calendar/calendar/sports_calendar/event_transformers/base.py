from abc import ABC, abstractmethod

import pandas as pd

from ..events import SportsEvent


class EventTransformer(ABC):
    """ Abstract base class for event transformers. """

    @abstractmethod
    def transform(self, series: pd.Series) -> SportsEvent:
        """ Transform a pandas Series into a SportsEvent. """
        raise NotImplementedError("Subclasses must implement this method.")

    def batch_transform(self, df: pd.DataFrame) -> list[SportsEvent]:
        """ Transform a DataFrame into a list of SportsEvents. """
        return [self.transform(series) for _, series in df.iterrows()]

    @staticmethod
    def clean_value(val):
        """ Convert NaN or missing values to None. """
        return None if pd.isna(val) else val

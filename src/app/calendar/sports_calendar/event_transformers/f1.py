import pandas as pd

from .base import EventTransformer
from ..events import F1Event


class F1EventTransformer(EventTransformer):
    """ Transformer for F1 events. """

    def transform(self, series: pd.Series) -> F1Event:
        """ Transform a pandas Series into an F1Event. """
        return F1Event(
            name=series['name'],
            session=series['session'],
            date_time=series['date_time'],
            city=self.clean_value(series.get('city')),
            country=self.clean_value(series.get('country'))
        )

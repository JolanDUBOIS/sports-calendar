import pandas as pd

from .base import EventTransformer
from ..events import FootballEvent


class FootballEventTransformer(EventTransformer):
    """ Transformer for football events. """

    def transform(self, series: pd.Series) -> FootballEvent:
        """ Transform a pandas Series into a FootballEvent. """
        return FootballEvent(
            home_team_name=series['home_team_name'],
            away_team_name=series['away_team_name'],
            date_time=series['date_time'],
            competition_name=self.clean_value(series.get('competition_name')),
            competition_abbreviation=self.clean_value(series.get('competition_abbreviation')),
            stage=self.clean_value(series.get('stage')),
            leg=self.clean_value(series.get('leg')),
            venue=self.clean_value(series.get('venue'))
        )

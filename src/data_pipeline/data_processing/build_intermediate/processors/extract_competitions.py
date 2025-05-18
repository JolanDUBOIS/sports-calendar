import pandas as pd

from . import logger
from .processor_base_class import Processor


class ExtractCompetitions(Processor):
    """ Extract competitions from matches data """

    source_columns = {
        "espn_matches": ["competition_id", "competition_name", "competition_abbreviation", "competition_midsizeName", "competition_slug"],
        "football_data_matches": ["competition_id", "competition_name", "competition_code", "competition_type"],
        "live_soccer_matches": ["competition"]
    }

    def process(self, data: pd.DataFrame, source_key: str, **kwargs) -> pd.DataFrame:
        """ TODO """
        columns = self.source_columns.get(source_key)
        if columns is None:
            logger.error(f"Source key {source_key} not found in source_columns.")
            raise ValueError(f"Source key {source_key} not found in source_columns.")
        output_data = data[columns]
        if kwargs.get("deduplicate", True):
            output_data = output_data.drop_duplicates()
        return output_data

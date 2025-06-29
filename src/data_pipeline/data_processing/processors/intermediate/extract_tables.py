import pandas as pd

from . import logger
from .. import Processor
from ..components import extract_json, reshape_matches, date_standardization


class ExtractionProcessor(Processor):
    """ Processor used to extract structured data from raw JSON sources. """

    sources_additional_processes = {
        "espn_football_matches": [reshape_matches, date_standardization],
        "espn_f1_events": [date_standardization],
        # "football_data_matches": [date_normalization],
    }

    def _run(self, sources: dict[str, list[dict]], source_key: str, output_key: str, **kwargs) -> pd.DataFrame:
        """ Run extraction processor for the given source key. """
        logger.info(f"Running ExtractionProcessor for source key: {source_key}")
        json_data = sources.get(source_key)
        if json_data is None:
            logger.error(f"No data found for source key: {source_key}")
            raise ValueError(f"No data found for source key: {source_key}")
        if not json_data:
            logger.info(f"No new data found for source key: {source_key}")
            return pd.DataFrame()
        self._check_json_data(json_data)

        data = extract_json(json_data, output_key, **kwargs)
        self._check_dataframe(data)

        additional_processes = self.sources_additional_processes.get(source_key, [])
        for process in additional_processes:
            data = process(data, source_key, **kwargs)
            self._check_dataframe(data)

        return data

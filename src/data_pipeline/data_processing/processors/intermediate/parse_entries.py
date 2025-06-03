import pandas as pd

from . import logger
from ..processor_base_class import Processor
from ..components import parse, date_normalization


class ParsingProcessor(Processor):
    """ Processor used to parse raw structured data into final schema. """

    sources_additional_processes = {
        "live_soccer_matches": [date_normalization],
    }

    def _run(self, sources: dict[str, pd.DataFrame], source_key: str, **kwargs) -> pd.DataFrame:
        """ Run parsing processor for the given source key. """
        logger.info(f"Running ParsingProcessor for source key: {source_key}")
        data = sources.get(source_key)
        if data is None:
            logger.error(f"No data found for source key: {source_key}")
            raise ValueError(f"No data found for source key: {source_key}")
        if data.empty:
            logger.info(f"No new data found for source key: {source_key}")
            return pd.DataFrame()
        self._check_dataframe(data)

        data = parse(data, source_key, **kwargs)
        self._check_dataframe(data)

        additional_processes = self.sources_additional_processes.get(source_key, [])
        for process in additional_processes:
            data = process(data, source_key, **kwargs)
            self._check_dataframe(data)

        return data

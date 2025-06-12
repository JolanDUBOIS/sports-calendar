import pandas as pd

from . import logger
from .. import Processor
from ..components import extract_table


class DerivationProcessor(Processor):
    """ Processor used to derive structured tables from a source DataFrame. """

    def _run(self, sources: dict[str, pd.DataFrame], source_key: str, output_key: str, **kwargs) -> pd.DataFrame:
        """ Run derivation processor for the given source and output key. """
        logger.info(f"Running DerivationProcessor for output key: {output_key}")
        data = sources.get(source_key)
        if data is None:
            logger.error(f"No data found for source key: {source_key}")
            raise ValueError(f"No data found for source key: {source_key}")
        if data.empty:
            logger.info(f"No new data found for source key: {source_key}")
            return pd.DataFrame()
        self._check_dataframe(data)

        data = extract_table(data, output_key, **kwargs)
        self._check_dataframe(data)
        return data

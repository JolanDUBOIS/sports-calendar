import pandas as pd

from . import logger
from .. import Processor
from ..components import create_mapping_table


class CanonicalMappingProcessor(Processor):
    """ Processor to create a canonical mapping DataFrame from multiple sources. """

    def _run(self, sources: dict[str, pd.DataFrame], similarity_table: str, **kwargs) -> pd.DataFrame:
        """ Run canonical mapping creation for the specified similarity table. """
        logger.info(f"Running CanonicalMappingProcessor for similarity table: {similarity_table}")
        data = create_mapping_table(sources, similarity_table, **kwargs)
        self._check_dataframe(data)
        return data

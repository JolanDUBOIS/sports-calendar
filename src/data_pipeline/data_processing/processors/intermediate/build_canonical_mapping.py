import pandas as pd

from . import logger
from .. import Processor
from ..components import create_mapping_table


class CanonicalMappingProcessor(Processor):
    """ Processor to create a canonical mapping DataFrame from multiple sources. """

    def _run(self, sources: dict[str, pd.DataFrame], entity_type: str, **kwargs) -> pd.DataFrame:
        """ Run canonical mapping creation for the specified entity type. """
        logger.info(f"Running CanonicalMappingProcessor for entity type: {entity_type}")
        data = create_mapping_table(sources, entity_type, **kwargs)
        self._check_dataframe(data)
        return data

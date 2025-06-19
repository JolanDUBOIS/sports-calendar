import pandas as pd

from . import logger
from .. import Processor
from ..components import create_similarity_table


class SimilarityProcessor(Processor):
    """ Processor to create a similarity DataFrame from multiple sources. """

    def _run(self, sources: dict[str, pd.DataFrame], entity_type: str, **kwargs) -> pd.DataFrame:
        """ Run similarity creation for the specified entiy type. """
        logger.info(f"Running SimilarityProcessor for entity type: {entity_type}")
        data = create_similarity_table(sources, entity_type, threshold=55, **kwargs)
        self._check_dataframe(data)
        return data

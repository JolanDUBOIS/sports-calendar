import pandas as pd

from . import logger
from .. import Processor
from ..components import create_registry_table


class RegistryProcessor(Processor):
    """ Processor to create a registry DataFrame from multiple sources. """

    def _run(self, sources: dict[str, pd.DataFrame], entity_type: str, **kwargs) -> pd.DataFrame:
        """ Run registry creation for the specified entity type. """
        logger.info(f"Running RegistryProcessor for entity type: {entity_type}")
        data = create_registry_table(sources, entity_type, **kwargs)
        self._check_dataframe(data)
        return data

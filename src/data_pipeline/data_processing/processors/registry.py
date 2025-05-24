import pandas as pd

from . import logger
from .processor_base_class import Processor
from .components import create_registry


class RegistryProcessor(Processor):
    """ TODO """

    def _run(self, sources: dict[str, pd.DataFrame], output_key: str, **kwargs) -> pd.DataFrame:
        """ TODO """
        logger.info(f"Running RegistryProcessor for output key: {output_key}")
        data = create_registry(sources, output_key, **kwargs)
        self._check_dataframe(data)
        return data

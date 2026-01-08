import pandas as pd

from . import logger
from .func import reshape_espn_matches
from ..base import Processor
from sports_calendar.sync_db.definitions.specs import ProcessingIOInfo


RESHAPE_FUNC_REGISTRY = {
    "reshape_espn_matches": reshape_espn_matches
}

class ReshapingProcessor(Processor):
    """ Processor to reshape data in a DataFrame. """
    config_filename = "reshaping"

    @classmethod
    def _run(cls, data: dict[str, pd.DataFrame], io_info: ProcessingIOInfo, **kwargs) -> pd.DataFrame:
        """ Reshape data according to the specified configuration. """
        df = data.get("data").copy()
        if df is None:
            logger.error("Input data not found in the provided data dictionary.")
            raise ValueError("Input data not found in the provided data dictionary.")
        
        config = cls.load_config(io_info.config_key)
        reshape_func = config.get("func")
        if not reshape_func:
            logger.error(f"No reshape function specified for config key: {io_info.config_key}")
            raise ValueError(f"No reshape function specified for config key: {io_info.config_key}")

        func = RESHAPE_FUNC_REGISTRY.get(reshape_func)
        if not func:
            logger.error(f"Reshape function '{reshape_func}' not found in registry.")
            raise ValueError(f"Reshape function '{reshape_func}' not found in registry.")

        return func(df)

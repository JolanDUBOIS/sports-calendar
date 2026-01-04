import pandas as pd

from . import logger
from .parse_livesoccertv import LSParser
from .parse_football_ranking import FRParser
from ..base import Processor
from src.specs import ProcessingIOInfo


PARSERS_REGISTRY = {
    "LSParser": LSParser,
    "FRParser": FRParser,
}

class ParsingProcessor(Processor):
    """ Processor to parse data in a DataFrame based on specified parsing instructions. """
    config_filename = "parsing"

    @classmethod
    def _run(cls, data: dict[str, pd.DataFrame], io_info: ProcessingIOInfo, **kwargs) -> pd.DataFrame:
        """ Parse the specified DataFrame according to the parsing specifications. """
        df = data.get("data").copy()
        if df is None:
            logger.error("Input data not found in the provided data dictionary.")
            raise ValueError("Input data not found in the provided data dictionary.")

        config = cls.load_config(io_info.config_key)
        parser_name = config.get("parser")
        parse_method_name = config.get("method")
        if parse_method_name is None:
            logger.error(f"No parsing method specified in the configuration for config key: {io_info.config_key}.")
            raise ValueError(f"No parsing method specified in the configuration for config key: {io_info.config_key}.")

        parser_class = PARSERS_REGISTRY.get(parser_name)
        parse_method = getattr(parser_class, parse_method_name, None)
        if parse_method is None:
            logger.error(f"Parsing method '{parse_method_name}' not found in the processor.")
            raise ValueError(f"Parsing method '{parse_method_name}' not found in the processor.")

        return parse_method(df)

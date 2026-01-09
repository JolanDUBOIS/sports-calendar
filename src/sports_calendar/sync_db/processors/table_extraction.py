from __future__ import annotations
from dataclasses import dataclass

import pandas as pd

from . import logger
from .base import Processor
from sports_calendar.sync_db.definitions.specs import ProcessingIOInfo


@dataclass
class ExtractionSpec:
    extraction_type: str
    columns_mapping: dict[str, str | list[str]]

    def __post_init__(self):
        valid_extraction_types = {"simple", "double"}
        if self.extraction_type not in valid_extraction_types:
            logger.error(f"Invalid extraction type: {self.extraction_type}. Expected one of {valid_extraction_types}.")
            raise ValueError(f"Invalid extraction type: {self.extraction_type}. Expected one of {valid_extraction_types}.")
        if self.extraction_type == "simple" and not all(isinstance(source, str) for source in self.columns_mapping.values()):
            logger.error("For 'simple' extraction type, all source fields must be strings.")
            raise TypeError("For 'simple' extraction type, all source fields must be strings.")
        if self.extraction_type == "double":
            if not all(isinstance(source, list) and len(source) == 2 for source in self.columns_mapping.values()):
                logger.error("For 'double' extraction type, all source fields must be lists of exactly two strings.")
                raise TypeError("For 'double' extraction type, all source fields must be lists of exactly two strings.")

    def is_simple(self) -> bool:
        """ Checks if the extraction specification is simple """
        return self.extraction_type == "simple"

    @classmethod
    def from_dict(cls, d: dict) -> ExtractionSpec:
        """ Creates an ExtractionSpec from a dictionary. """
        return cls(
            extraction_type=d["extraction_type"],
            columns_mapping=d["columns_mapping"]
        )

class TableExtractionProcessor(Processor):
    """ Processor to extract tables from a DataFrame based on specified extraction instructions. """
    config_filename = "table_extraction"

    @classmethod
    def _run(cls, data: dict[str, pd.DataFrame], io_info: ProcessingIOInfo, **kwargs) -> pd.DataFrame:
        """ Extract tables from the specified DataFrame according to the extraction specifications. """
        df = data.get("data").copy()
        if df is None:
            logger.error("Input data not found in the provided data dictionary.")
            raise ValueError("Input data not found in the provided data dictionary.")

        config = cls.load_config(io_info.config_key)
        spec = ExtractionSpec.from_dict(config)

        if df.empty:
            logger.warning("Input DataFrame is empty. No extraction will be performed.")
            return df

        if spec.is_simple():
            return cls._simple_extraction(df, spec.columns_mapping)
        else:
            return cls._double_extraction(df, spec.columns_mapping)

    @staticmethod
    def _simple_extraction(
        df: pd.DataFrame,
        columns_mapping: dict[str, str]
    ) -> pd.DataFrame:
        """ Extract specified columns from the DataFrame. """
        _df = df[list(columns_mapping.values())].rename(columns={v: k for k, v in columns_mapping.items()})
        _df = _df.drop_duplicates()
        return _df

    @staticmethod
    def _double_extraction(
        df: pd.DataFrame,
        columns_mapping: dict[str, list[str]]
    ) -> pd.DataFrame:
        """
        Extracts and renames columns from the DataFrame using a columns mapping.
        Each key in columns_mapping is the target column name, and the value is a list of source column names.
        """
        target_columns = columns_mapping.keys()
        output_data = pd.DataFrame(columns=target_columns)
        for columns in zip(*columns_mapping.values()):
            logger.debug(f"Extracting columns: {columns}")
            if not all(col in df.columns for col in columns):
                logger.error(f"One of the columns {columns} is not present in the dataframe.")
                raise ValueError(f"One of the columns {columns} is not present in the dataframe.")
            new_rows = df[list(columns)].rename(columns=dict(zip(columns, target_columns)))
            output_data = pd.concat([output_data, new_rows], ignore_index=True)
        output_data = output_data.drop_duplicates()
        return output_data

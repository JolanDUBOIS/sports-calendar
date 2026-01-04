from __future__ import annotations
from dataclasses import dataclass

import pandas as pd

from . import logger
from .base import Processor
from src.specs import ProcessingIOInfo


@dataclass
class RemapSpec:
    target_field: str
    using_table: str
    match_on: str
    replace_by: str

    @classmethod
    def from_dict(cls, d: dict) -> RemapSpec:
        """ Creates a RemapSpec from a dictionary. """
        return cls(
            target_field=d["target_field"],
            using_table=d["using_table"],
            match_on=d["match_on"],
            replace_by=d["replace_by"]
        )

class RemappingProcessor(Processor):
    """ Processor to remap columns in a DataFrame based on specified remap instructions. """
    config_filename = "remapping"

    @classmethod
    def _run(cls, data: dict[str, pd.DataFrame], io_info: ProcessingIOInfo, **kwargs) -> pd.DataFrame:
        """ Remap columns in the specified DataFrame according to the remap specifications. """
        df = data.get("data").copy()
        if df is None:
            logger.error("Input data not found in the provided data dictionary.")
            raise ValueError("Input data not found in the provided data dictionary.")

        config = cls.load_config(io_info.config_key)
        specs = [RemapSpec.from_dict(element) for element in config]

        for spec in specs:
            if spec.target_field not in df.columns:
                logger.error(f"Target field '{spec.target_field}' not found in DataFrame columns.")
                raise ValueError(f"Target field '{spec.target_field}' not found in DataFrame columns.")
            if spec.using_table not in data:
                logger.error(f"Using table '{spec.using_table}' not found in mapping tables.")
                raise ValueError(f"Using table '{spec.using_table}' not found in mapping tables.")

            mapping_table = data[spec.using_table].copy()
            if spec.match_on not in mapping_table.columns or spec.replace_by not in mapping_table.columns:
                logger.error(f"Columns '{spec.match_on}' or '{spec.replace_by}' not found in using table '{spec.using_table}'.")
                raise ValueError(f"Columns '{spec.match_on}' or '{spec.replace_by}' not found in using table '{spec.using_table}'.")
        
            logger.debug(f"Remapping '{spec.target_field}' using '{spec.using_table}' with match on '{spec.match_on}' and replace by '{spec.replace_by}'")
            mapping_table = mapping_table.rename(columns={spec.match_on: spec.match_on + "__remap", spec.replace_by: spec.replace_by + "__remap"})
            df = df.merge(
                mapping_table[[spec.match_on + "__remap", spec.replace_by + "__remap"]],
                left_on=spec.target_field,
                right_on=spec.match_on + "__remap",
                how='left'
            )
            df[spec.target_field] = df[spec.replace_by + "__remap"]
            df = df.drop(columns=[spec.match_on + "__remap", spec.replace_by + "__remap"])
        return df

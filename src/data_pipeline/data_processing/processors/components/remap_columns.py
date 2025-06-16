from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path

import yaml
import pandas as pd

from . import logger


# Specifications for remapping columns

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

def load_remap_specs_from_yml(path: str | Path) -> dict[str, list[RemapSpec]]:
    path = Path(path)
    with path.open(mode='r') as file:
        raw_data = yaml.safe_load(file)
    
    if not isinstance(raw_data, dict):
        logger.error(f"Invalid YAML format in {path}. Expected a dictionary.")
        raise TypeError(f"Invalid YAML format in {path}. Expected a dictionary.")

    remap_specs = {}
    for key, value in raw_data.items():
        if not isinstance(value, list):
            logger.error(f"Invalid format for remap spec '{key}'. Expected a list.")
            raise TypeError(f"Invalid format for remap spec '{key}'. Expected a list.")
        remap_specs[key] = [RemapSpec.from_dict(item) for item in value]

    return remap_specs

REMAP_SPECIFICATIONS = load_remap_specs_from_yml(Path(__file__).parent / "config" / "remap_columns.yml")

# Remap columns function

def remap_columns(data: pd.DataFrame, key: str, mapping_tables: dict[str, pd.DataFrame], **kwargs) -> pd.DataFrame:
    """ Remaps columns in the DataFrame based on the remap specifications. """
    if key not in REMAP_SPECIFICATIONS:
        logger.error(f"No remap specifications found for key: {key}")
        raise ValueError(f"No remap specifications found for key: {key}")

    specs = REMAP_SPECIFICATIONS[key]
    for spec in specs:
        if spec.target_field not in data.columns:
            logger.error(f"Target field '{spec.target_field}' not found in DataFrame columns.")
            raise ValueError(f"Target field '{spec.target_field}' not found in DataFrame columns.")
        if spec.using_table not in mapping_tables:
            logger.error(f"Using table '{spec.using_table}' not found in mapping tables.")
            raise ValueError(f"Using table '{spec.using_table}' not found in mapping tables.")
        mapping_table = mapping_tables[spec.using_table]
        if spec.match_on not in mapping_table.columns or spec.replace_by not in mapping_table.columns:
            logger.error(f"Columns '{spec.match_on}' or '{spec.replace_by}' not found in using table '{spec.using_table}'.")
            raise ValueError(f"Columns '{spec.match_on}' or '{spec.replace_by}' not found in using table '{spec.using_table}'.")
        # Rename mapping columns to avoid conflicts
        logger.debug(f"Remapping '{spec.target_field}' using '{spec.using_table}' with match on '{spec.match_on}' and replace by '{spec.replace_by}'")
        mapping_table = mapping_table.rename(columns={spec.match_on: spec.match_on + "__remap", spec.replace_by: spec.replace_by + "__remap"})
        data = data.merge(
            mapping_table[[spec.match_on + "__remap", spec.replace_by + "__remap"]],
            left_on=spec.target_field,
            right_on=spec.match_on + "__remap",
            how='left'
        )
        logger.debug(f"Head of merged DataFrame:\n{data.head()}")
        data[spec.target_field] = data[spec.replace_by + "__remap"]
        data = data.drop(columns=[spec.match_on + "__remap", spec.replace_by + "__remap"])
    return data

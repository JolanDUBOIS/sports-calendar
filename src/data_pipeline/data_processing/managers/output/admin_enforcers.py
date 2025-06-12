from abc import ABC
from typing import TYPE_CHECKING

import pandas as pd

from . import logger
from .enforcers import OutputEnforcer
if TYPE_CHECKING:
    from .admin_specs import AdminRuleSpec, ForceMatchSpec, BlockMatchSpec


class AdminEnforcer(OutputEnforcer, ABC):
    """ Enforces admin rules on the DataFrame based on admin specifications. """

    def __init__(self, spec: AdminRuleSpec):
        """ Initialize the enforcer with the specification. """
        self.spec = spec

class ForceMatchEnforcer(AdminEnforcer):
    """ Enforces force match rules on the DataFrame. """

    def _apply(self, df: pd.DataFrame) -> pd.DataFrame:
        """ TODO """
        self.spec: ForceMatchSpec
        for entity in self.spec.entities:
            df = df[~((df["source_A"] == entity.source) & (df["id_A"] == entity.source_id))]
            df = df[~((df["source_B"] == entity.source) & (df["id_B"] == entity.source_id))]
        for pair in self.spec.entities.all_pairs():
            new_row = {
                "id_A": pair[0].source_id,
                "id_B": pair[1].source_id,
                "source_A": pair[0].source,
                "source_B": pair[1].source,
                "similarity_score": 100
            }
            df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
        return df.reset_index(drop=True)

    @staticmethod
    def _check_df(df: pd.DataFrame) -> None:
        """ Check if df is valid, raise error if not. """
        super()._check_df(df)
        expected_cols = ["id_A", "id_B", "source_A", "source_B", "similarity_score"]
        if not all(col in df.columns for col in expected_cols):
            logger.error(f"DataFrame is missing required columns: {expected_cols}")
            raise ValueError(f"DataFrame is missing required columns: {expected_cols}")

class BlockMatchEnforcer(AdminEnforcer):
    """ Enforces block match rules on the DataFrame. """

    def _apply(self, df: pd.DataFrame) -> pd.DataFrame:
        """ TODO """
        self.spec: BlockMatchSpec
        for pair in self.spec.entities.all_pairs():
            df = df[
                ~((df["source_A"] == pair[0].source) & (df["id_A"] == pair[0].source_id) &
                  (df["source_B"] == pair[1].source) & (df["id_B"] == pair[1].source_id))
            ]
        return df.reset_index(drop=True)

from __future__ import annotations
import itertools

import pandas as pd

from . import logger
from .similarity_score import calculate_list_similarity_score
from .specs import SourceTableSpec, REGISTRY_SPECS, REFERENCE_SPECS


class SourceTable:
    """ TODO """

    def __init__(self, df: pd.DataFrame, source_table_spec: SourceTableSpec):
        """ TODO """
        self.source_df = df

        self.id_col = source_table_spec.id_col
        self.match_columns = source_table_spec.match_columns
    
    def get_prepared_df(self, suffix: str = "") -> pd.DataFrame:
        """ TODO """
        _df = self.source_df.copy()

        cols = [self.id_col, "source"] + self.match_columns
        new_cols = [f"id_{suffix}", f"source_{suffix}"] + [f"match_{k}_{suffix}" for k in range(len(self.match_columns))]

        for src_col, new_col in zip(cols, new_cols):
            _df[new_col] = _df[src_col]

        return _df[new_cols]

class MergedTable:
    """ TODO """

    def __init__(self, df: pd.DataFrame, generic_tokens: list[str] = None, **kwargs):
        """ TODO """
        self.df = df
        self.generic_tokens = generic_tokens or []
        self.left_match_cols, self.right_match_cols = self._get_match_columns()

    def _get_match_columns(self) -> tuple[list[str], list[str]]:
        """ TODO """
        cols = self.df.columns
        match_cols = [col for col in cols if col.startswith("match_")]
        left_match_cols = [col for col in match_cols if col.endswith("_A")]
        right_match_cols = [col for col in match_cols if col.endswith("_B")]
        return left_match_cols, right_match_cols

    def compute_similarity_score(self, threshold: float) -> pd.DataFrame:
        """ TODO """
        output_df = self.df.copy()
        output_df['similarity_score'] = output_df.apply(
            lambda row: calculate_list_similarity_score(
                row[self.left_match_cols].tolist(),
                row[self.right_match_cols].tolist(),
                tokens_to_remove=self.generic_tokens
            ),
            axis=1
        )
        output_df = output_df[output_df['similarity_score'] >= threshold].reset_index(drop=True)
        output_df = output_df[['id_A', 'id_B', 'source_A', 'source_B', 'similarity_score']]
        return output_df

    @classmethod
    def from_source_tables(cls, left: SourceTable, right: SourceTable, suffixes: tuple[str, str] = ("A", "B"), generic_tokens: list[str] = None, **kwargs) -> MergedTable:
        """ TODO """
        df1 = left.get_prepared_df(suffix=suffixes[0])
        df2 = right.get_prepared_df(suffix=suffixes[1])
        return cls(pd.merge(df1, df2, how='cross'), generic_tokens, **kwargs)

class RegistryCreation:
    """ TODO """

    def __init__(self, sources: dict[str, pd.DataFrame], entity_type: str):
        """ TODO """
        self.registry_spec = REGISTRY_SPECS.get(entity_type)
        self.source_tables = {
            source_name: SourceTable(df, self.registry_spec.get(source_name))
            for source_name, df in sources.items()
        }

    @property
    def _source_pairs(self) -> list[tuple[str, str]]:
        """ TODO """
        return list(itertools.combinations(list(self.source_tables.keys()), 2))

    def run(self, threshold: float, **kwargs) -> pd.DataFrame:
        """ TODO """
        registries = []
        for pair in self._source_pairs:
            left_source, right_source = self.source_tables[pair[0]], self.source_tables[pair[1]]
            merged_table = MergedTable.from_source_tables(
                left_source, right_source,
                generic_tokens=self.registry_spec.generic_tokens
            )
            pair_registry = merged_table.compute_similarity_score(threshold=threshold)
            registries.append(pair_registry)
        if not registries:
            logger.error(f"No registries created for entity type '{self.registry_spec.entity_type}'.")
            raise ValueError(f"No registries created for entity type '{self.registry_spec.entity_type}'.")
        return pd.concat(registries, ignore_index=True)

def create_registry(sources: dict[str, pd.DataFrame], entity_type: str, threshold: float = 50, **kwargs) -> pd.DataFrame:
    """ TODO """
    return RegistryCreation(sources, entity_type).run(threshold=threshold, **kwargs)

from __future__ import annotations
import hashlib

import pandas as pd

from . import logger
from .specs import CoalesceRuleSpecs, REGISTRY_SPECS
from ..similarity.specs import SourceTableSpec, SimilaritySpec, SIMILARITY_SPECS


class SourceEntityTable:
    """ TODO """

    def __init__(self, name: str, df: pd.DataFrame, source_spec: SourceTableSpec):
        """ TODO """
        self.name = name

        _df = df.copy()
        _df.columns = [f"{name}.{col}" for col in _df.columns]
        self.df = _df

        self.id_col = f"{name}.{source_spec.id_col}"
        self._check_valid_spec(source_spec)

    def _check_valid_spec(self, spec: SourceTableSpec) -> None:
        """ Check if the source specification is valid. """
        if not spec.table_name == self.name:
            logger.error(f"Source table name '{spec.table_name}' does not match the provided name '{self.name}'.")
            raise ValueError(f"Source table name '{spec.table_name}' does not match the provided name '{self.name}'.")
        if not self.id_col in self.df.columns:
            logger.error(f"ID column '{self.id_col}' not found in DataFrame columns: {self.df.columns.tolist()}.")
            raise KeyError(f"ID column '{self.id_col}' not found in DataFrame columns: {self.df.columns.tolist()}.")

    @property
    def source_col(self) -> str:
        """ Get the source column name. """
        return f"{self.name}.source"

class SourceEntityTableCollection:
    """ Collection of source entity tables. """

    def __init__(self, tables: list[SourceEntityTable]):
        """ Initialize the collection with a list of SourceEntityTables. """
        self.tables = tables

    def get(self, name: str) -> SourceEntityTable:
        """ Get a SourceEntityTable by its name. """
        for table in self.tables:
            if table.name == name:
                return table
        logger.error(f"SourceEntityTable with name '{name}' not found.")
        raise KeyError(f"SourceEntityTable with name '{name}' not found.")

    @classmethod
    def from_dict(cls, sources: dict[str, pd.DataFrame], specs: SimilaritySpec) -> SourceEntityTableCollection:
        """ Create a SourceEntityTableCollection from a dictionary of DataFrames and a SimilaritySpec. """
        tables = []
        for src_name, df in sources.items():
            if not specs.is_in(src_name):
                continue
            source_spec = specs.get(src_name)
            tables.append(SourceEntityTable(name=src_name, df=df, source_spec=source_spec))
        return cls(tables=tables)


class CanonicalMappingTable:
    """ TODO """

    def __init__(self, name: str, df: pd.DataFrame, **kwargs):
        """ TODO """
        self.name = name
        self.df = df.copy()
        
        self.joined_tables = kwargs.get("joined_tables", [])

    def join(self, other: SourceEntityTable) -> CanonicalMappingTable:
        """ Join the canonical mapping table with another source entity table. """
        logger.debug(f"Joining {self.name} with {other.name}.")
        merged_df = pd.merge(
            self.df,
            other.df,
            how="outer",
            left_on=["source_id", "source"],
            right_on=[other.id_col, other.source_col],
        )
        logger.debug(f"Merged DataFrame columns: {merged_df.columns.tolist()}")
        return CanonicalMappingTable(name=self.name, df=merged_df, joined_tables=self.joined_tables + [other.name])

    def join_all(self, other_tables: SourceEntityTableCollection) -> CanonicalMappingTable:
        """ Join the canonical mapping table with all source entity tables in the collection. """
        merged_table = self
        for table in other_tables.tables:
            merged_table = merged_table.join(table)
            logger.debug(f"Head of merged DataFrame:\n{merged_table.df.head(20)}")
        return merged_table


def apply_coalesce_rules(df: pd.DataFrame, rules: CoalesceRuleSpecs) -> pd.DataFrame:
    """ Apply coalesce rules to the DataFrame. """
    logger.debug(f"Applying coalesce rules: {rules}")
    cols = ["id"]
    for rule in rules.rules:
        df[rule.col_name] = df[rule.source_cols].bfill(axis=1).iloc[:, 0]
        cols.append(rule.col_name)
    return df[cols]

def drop_na(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    """ Drop rows with NA values in the specified columns. """
    if not columns:
        return df
    logger.debug(f"Dropping rows with NA in columns: {columns}")
    return df.dropna(subset=columns).reset_index(drop=True)

def force_unique(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    """ Force unique values in the specified columns by dropping duplicates. """
    if not columns:
        return df
    logger.debug(f"Forcing unique values in columns: {columns}")
    return df.drop_duplicates(subset=columns).reset_index(drop=True)

def create_registry_table(sources: dict[str, pd.DataFrame], entity_type: str, **kwargs) -> pd.DataFrame:
    """ Create a registry table from the provided sources. """
    registry_spec = REGISTRY_SPECS.get(entity_type)
    source_specs = SIMILARITY_SPECS.get(entity_type) # We are only interested in sources and not generic_tokens

    canonical_mapping_table_name = registry_spec.canonical_mapping_table
    canonical_mapping_table = CanonicalMappingTable(
        name=canonical_mapping_table_name,
        df=sources[canonical_mapping_table_name]
    )

    source_tables = SourceEntityTableCollection.from_dict(sources, source_specs)

    merged_df = canonical_mapping_table.join_all(source_tables)
    df = apply_coalesce_rules(merged_df.df, registry_spec.coalesce_rules)
    df = drop_na(df, ["id"] + registry_spec.coalesce_rules.get_non_nullable_col_name())
    df = force_unique(df, ["id"])

    return df

import itertools

import pandas as pd

from . import logger
from .similarity_score import calculate_list_similarity_score
from .registry_specs import REGISTRY_SPECS


class RegistrySource:
    """ TODO """

    def __init__(self, source_key: str, df: pd.DataFrame, registry_key: str):
        """ TODO """
        self.source_key = source_key
        self.source_df = df
        self.source = df["source"].iloc[0] if not df.empty else None

        self.registry_key = registry_key
        self.src_reg_spec = REGISTRY_SPECS.get_source_spec(registry_key, source_key)

    def get_registry_ready_df(self, suffix: str) -> pd.DataFrame:
        """ TODO """ 
        _df = self.source_df.copy()

        cols = [self.src_reg_spec.id_col, "source"] + self.src_reg_spec.column_variants
        new_cols = [f"id_{suffix}", f"source_{suffix}"] + [f"{col}_{suffix}" for col in self.src_reg_spec.column_variants]

        for src_col, new_col in zip(cols, new_cols):
            _df[new_col] = _df[src_col]

        return _df

    def column_variants(self, suffix: str) -> list[str]:
        """ TODO """
        return [f"{col}_{suffix}" for col in self.src_reg_spec.column_variants]

class RegistryCreator:
    """ TODO """

    def __init__(self, sources: dict[str, pd.DataFrame], output_key: str):
        """ TODO """
        self.sources = {
            source_key: RegistrySource(source_key, df, output_key)
            for source_key, df in sources.items()
        }
        self.registry_key = output_key

    def __call__(self, threshold: float = 50, **kwargs) -> pd.DataFrame:
        """ TODO """
        registry = self._get_full_registry()
        return registry[registry["similarity_score"] >= threshold].reset_index(drop=True)

    def _get_full_registry(self) -> pd.DataFrame:
        """ TODO """
        pairs = self._get_pairs(list(self.sources.keys()))

        registries = []
        for pair in pairs:
            source1, source2 = self.sources[pair[0]], self.sources[pair[1]]
            df1, df2 = source1.get_registry_ready_df(suffix="A"), source2.get_registry_ready_df(suffix="B")
            logger.debug(f"Columns df1: {df1.columns.tolist()}, df2: {df2.columns.tolist()}")

            merged_df = pd.merge(df1, df2, how='cross')
            merged_df["similarity_score"] = merged_df.apply(
                lambda row: calculate_list_similarity_score(
                    [row[col] for col in source1.column_variants(suffix="A")],
                    [row[col] for col in source2.column_variants(suffix="B")],
                    REGISTRY_SPECS.get(self.registry_key).generic_tokens
                ),
                axis=1
            )
            logger.debug(f"Processed pair {pair} with merged shape: {merged_df.shape}")
            logger.debug(f"Sample merged_df:\n{merged_df.head(20)}")

            if not merged_df.empty:
                registries.append(merged_df[["id_A", "id_B", "source_A", "source_B", "similarity_score"]])
        
        registry = pd.concat(registries, ignore_index=True)
        logger.debug(f"Final registry shape: {registry.shape}")
        return registry            

    @staticmethod
    def _get_pairs(source_keys: list[str]) -> list[tuple[str, str]]:
        """ TODO """
        return list(itertools.combinations(source_keys, 2))

def create_registry(
    sources: dict[str, pd.DataFrame],
    output_key: str,
    threshold: float = 55,
    **kwargs
) -> pd.DataFrame:
    """ Create a registry of similar records from multiple sources filtered by a similarity threshold. """
    creator = RegistryCreator(sources, output_key)
    return creator(threshold, **kwargs)

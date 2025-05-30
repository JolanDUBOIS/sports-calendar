import itertools

import pandas as pd

from . import logger
from .constants import REGISTRY_CONSTANTS
from .similarity_score import calculate_list_similarity_score


def create_registry(
    sources: dict[str, pd.DataFrame],
    output_key: str,
    threshold: float = 50,
    **kwargs
) -> dict[str, pd.DataFrame]:
    """ Create a registry of similar records from multiple sources filtered by a similarity threshold. """
    constants = REGISTRY_CONSTANTS.get(output_key)
    if not constants:
        logger.error(f"Output key {output_key} not found in REGISTRY_CONSTANTS.")
        raise ValueError(f"Output key {output_key} not found in REGISTRY_CONSTANTS.")
    generic_tokens = constants.get("generic_tokens", [])
    registry_parameters = constants.get("registry_parameters", {})
    _validate_params(
        sources=sources,
        registry_parameters=registry_parameters,
        generic_tokens=generic_tokens
    )
    registry = create_similarity_table(
        sources=sources,
        registry_parameters=registry_parameters,
        generic_tokens=generic_tokens
    )
    return registry[registry["_similarity_score"] >= threshold].reset_index(drop=True)

def create_similarity_table(
    sources: dict[str, pd.DataFrame],
    registry_parameters: dict[str, dict],
    generic_tokens: list[str] = None
) -> pd.DataFrame:
    """ Build a similarity table comparing pairs of dataframes from different sources. """
    logger.debug("Starting similarity table creation...")
    sources_with_prefix = {}
    for key, df in sources.items():
        try:
            source_key = df["source"].iloc[0]
        except KeyError:
            logger.error(f"Key 'source' not found in DataFrame for source '{key}'.")
            raise KeyError(f"Key 'source' not found in DataFrame for source '{key}'.")
        except IndexError:
            logger.info(f"No data found for source '{key}'. Skipping.")
            continue
        registry_parameters[key]["source"] = source_key
        registry_parameters[key]["column_variants"] = [f"{source_key}_{col}" for col in registry_parameters[key]["column_variants"]]
        registry_parameters[key]["id_col"] = f"{source_key}_{registry_parameters[key]['id_col']}"
        df.columns = [f'{source_key}_{col}' for col in df.columns]
        sources_with_prefix[key] = df
        logger.debug(f"Prepared source '{key}' with prefix '{source_key}'. Columns: {df.columns.tolist()}")

    registries = []
    pairs = list(itertools.combinations(sources_with_prefix.keys(), 2))
    logger.debug(f"Generated {len(pairs)} source pairs: {pairs}")
    for pair in pairs:
        source_keyA = pair[0]
        source_keyB = pair[1]
        dfA = sources_with_prefix[source_keyA]
        dfB = sources_with_prefix[source_keyB]
        logger.debug(f"Processing pair: {source_keyA} x {source_keyB}")
        logger.debug(f"dfA:\n{dfA.head(20)}")
        logger.debug(f"dfB:\n{dfB.head(20)}")
        
        id_colA = registry_parameters[source_keyA]["id_col"]
        id_colB = registry_parameters[source_keyB]["id_col"]

        pair_registry = _create_similarity_table(
            dfA,
            dfB,
            registry_parameters[source_keyA]["column_variants"],
            registry_parameters[source_keyB]["column_variants"],
            generic_tokens,
            id_colA=id_colA,
            id_colB=id_colB
        )

        pair_registry["sourceA"] = registry_parameters[source_keyA]["source"]
        pair_registry["sourceB"] = registry_parameters[source_keyB]["source"]
        pair_registry["idA"] = pair_registry[id_colA]
        pair_registry["idB"] = pair_registry[id_colB]

        pair_registry = pair_registry[["idA", "idB", "sourceA", "sourceB", "_similarity_score"]]
        logger.debug(f"Pair registry shape: {pair_registry.shape}")
        logger.debug(f"Pair registry:\n{pair_registry.head(20)}")

        if not pair_registry.empty:
            registries.append(pair_registry)

    registry = pd.concat(registries, ignore_index=True)
    logger.debug(f"Final registry shape: {registry.shape}")
    return registry

def _create_similarity_table(
    dfA: pd.DataFrame,
    dfB: pd.DataFrame,
    column_variants_A: list[str],
    column_variants_B: list[str],
    generic_tokens: list[str] = None,
    **kwargs
) -> pd.DataFrame:
    """ Compute similarity scores for all combinations of rows between two dataframes. """
    merged_df = pd.merge(dfA, dfB, how="cross")
    merged_df["_similarity_score"] = merged_df.apply(
        lambda row: calculate_list_similarity_score(
            [row[col] for col in column_variants_A],
            [row[col] for col in column_variants_B],
            generic_tokens
        ),
        axis=1
    )
    return merged_df

def _validate_params(
    sources: dict[str, pd.DataFrame],
    registry_parameters: dict[str, dict],
    generic_tokens: list[str]
) -> None:
    """ Validate input parameters for source data, registry parameters, and generic tokens. """
    if not isinstance(sources, dict):
        logger.error("Sources should be a dictionary.")
        raise ValueError("Sources should be a dictionary.")
    if not isinstance(registry_parameters, dict):
        logger.error("Registry parameters should be a dictionary.")
        raise ValueError("Registry parameters should be a dictionary.")
    if not isinstance(generic_tokens, list):
        logger.error("Generic tokens should be a list.")
        raise ValueError("Generic tokens should be a list.")

    for key in sources.keys():
        if key not in registry_parameters:
            logger.warning(f"Key {key} not found in registry parameters.")

import pandas as pd 


def concatenate_unique_rows(
    events1: pd.DataFrame,
    events2: pd.DataFrame,
    columns_to_compare: list[str] = None
) -> pd.DataFrame:
    """ TODO """
    concatenated_df = pd.concat([events1, events2], ignore_index=True)
    concatenated_df.drop_duplicates(inplace=True, subset=columns_to_compare)
    return concatenated_df

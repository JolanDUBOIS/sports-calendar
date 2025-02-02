import pandas as pd 


def concatenate_unique_rows(
    events1: pd.DataFrame,
    events2: pd.DataFrame
) -> pd.DataFrame:
    """ TODO """
    concatenated_df = pd.concat([events1, events2], ignore_index=True)
    concatenated_df.drop_duplicates(inplace=True)
    return concatenated_df

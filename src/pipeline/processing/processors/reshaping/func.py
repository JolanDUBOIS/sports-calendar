import numpy as np
import pandas as pd


def reshape_espn_matches(df: pd.DataFrame) -> pd.DataFrame:
    """ Reshape ESPN matches dataframe to have separate home and away team attributes. """
    home_is_A = df["team_A_homeAway"] == "home"
    home = np.where(home_is_A, "A", "B")
    away = np.where(home_is_A, "B", "A")

    for attr in [
        "id", "winner", "score", "abbreviation", "displayName",
        "shortDisplayName", "name", "location", "venue_id"
    ]:
        df[f"home_team_{attr}"] = [
            df.at[i, f"team_{h}_{attr}"] for i, h in zip(df.index, home)
        ]
        df[f"away_team_{attr}"] = [
            df.at[i, f"team_{a}_{attr}"] for i, a in zip(df.index, away)
        ]

    df.drop(
        columns=[col for col in df.columns if col.startswith("team_")],
        inplace=True
    )

    return df
import pandas as pd

from .models import TeamsTable, CompetitionsTable


def get_clean_matches(matches: pd.DataFrame) -> pd.DataFrame:
    """ TODO """
    teams_df = TeamsTable.query().add_prefix('team_')
    competitions_df = CompetitionsTable.query().add_prefix('competition_')

    full_matches = matches.merge(
        teams_df.add_prefix('home_'),
        left_on='home_team_id',
        right_on='home_team_id',
        how='left'
    ).merge(
        teams_df.add_prefix('away_'),
        left_on='away_team_id',
        right_on='away_team_id',
        how='left'
    ).merge(
        competitions_df,
        left_on='competition_id',
        right_on='competition_id',
        how='left'
    )

    return full_matches[["home_team_name", "away_team_name", "competition_name", "competition_abbreviation", "date_time", "stage", "leg", "venue"]]

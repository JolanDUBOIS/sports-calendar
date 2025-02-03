from pathlib import Path

from src.football_data_source_deprecated import FootballDataConnector


def fetch_and_store_teams(connector: FootballDataConnector, file_path: Path):
    """ TODO """
    teams_df = connector.request_teams()
    teams_df.to_csv(file_path, index=False)

def fetch_and_store_areas(connector: FootballDataConnector, file_path: Path):
    """ TODO """
    areas_df = connector.request_areas()
    areas_df.to_csv(file_path, index=False)

def fetch_and_store_competitions(connector: FootballDataConnector, file_path: Path):
    """ TODO """
    competitions_df = connector.request_competitions()
    competitions_df.to_csv(file_path, index=False)

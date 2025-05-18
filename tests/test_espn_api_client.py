# import pytest
# import pandas as pd

# from src.sources.api import ESPNApiClient


# def test__get_matches():
#     """ TODO """
#     api_client = ESPNApiClient()
    
#     date_strs = ["20250420", "20250524", "20250429"]
#     competition_slugs = ["eng.1", "fra.coupe_de_france", "uefa.champions"]
    
#     for competition_slug, date_str in zip(competition_slugs, date_strs):
#         matches = api_client._get_matches(competition_slug, date_str)

#         assert isinstance(matches, pd.DataFrame), f"Expected DataFrame, got {type(matches)}"
#         assert not matches.empty, f"Expected non-empty DataFrame for {competition_slug} on {date_str}"

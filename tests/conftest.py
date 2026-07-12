import os

import pytest

from sports_calendar.core.selection import Selection


@pytest.fixture(scope="session", autouse=True)
def setup_test_environment():
    """Automatically set environment variables for all tests."""
    os.environ["SPORTINDEX_RECORD_MODE"] = os.environ.get("SPORTINDEX_RECORD_MODE") or "auto"
    os.environ["SPORTINDEX_FIXTURES_DIR"] = "tests/mock_data"

    yield

    os.environ.pop("SPORTINDEX_RECORD_MODE", None)
    os.environ.pop("SPORTINDEX_FIXTURES_DIR", None)


@pytest.fixture
def football_competition_ids():
    return [
        "trnc:7",   # Champions League
        "trnc:17",  # Premier League
        "trnc:34",  # Ligue 1
        "trnc:679", # Europa League
        "trnc:8",   # La Liga
        "trnc:23"   # Serie A
    ]

@pytest.fixture
def football_team_ids():
    return [
        "t-cpt:1644", # PSG
        "t-cpt:1653", # Monaco
        "t-cpt:1649", # Olympique Lyonnais
        "t-cpt:42",   # Arsenal
        "t-cpt:17",   # Manchester City
        "t-cpt:44",   # Liverpool
        "t-cpt:2829", # Real Madrid
        "t-cpt:2817", # Barcelona
        "t-cpt:2836", # Atletico Madrid
        "t-cpt:2672", # Bayern Munich
    ]

@pytest.fixture
def raw_selection_data(football_competition_ids, football_team_ids):
    return {
        "name": "dev",
        "items": [
            {
                "name": "item-1",
                "sport_id": 1,
                "uid": "it-001",
                "filters": [
                    {
                        "name": "item-1-filter-1",
                        "sport_id": 1,
                        "uid": "it-001-filt-001",
                        "fields": {"filter_type": "empty"}
                    },
                    {
                        "name": "item-1-filter-2",
                        "sport_id": 1,
                        "uid": "it-001-filt-002",
                        "fields": {
                            "filter_type": "min_ranking",
                            "ranking": 5,
                            "competition_ids": football_competition_ids,
                            "selection_rule": {"reference": None, "rule": "any"}
                        }
                    },
                    {
                        "name": "item-1-filter-3",
                        "sport_id": 1,
                        "uid": "it-001-filt-003",
                        "fields": {
                            "filter_type": "competitors",
                            "competitor_ids": football_team_ids,
                            "selection_rule": {"reference": "t-cpt:40", "rule": "opponent"}
                        }
                    }
                ]
            },
            {
                "name": "item-2",
                "sport_id": 11,
                "uid": "it-002",
                "filters": [
                    {
                        "name": "item-2-filter-1",
                        "sport_id": 11,
                        "uid": "it-002-filt-001",
                        "created_at": "2026-01-30T19:57:02",
                        "fields": {
                            "filter_type": "sessions",
                            "competition_id": "stgc:40",
                            "sessions": [10, 6]  # SPRINT_RACE, RACE
                        }
                    }
                ]
            }
        ]
    }

@pytest.fixture
def sample_selection(raw_selection_data):
    # This supposes that Selection.from_dict is working correctly
    return Selection.from_dict(raw_selection_data)

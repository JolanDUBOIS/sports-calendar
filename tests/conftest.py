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
        10000000007, # Champions League
        10000000017, # Premier League
        10000000034, # Ligue 1
        10000000679, # Europa League
        10000000008, # La Liga
        10000000023  # Serie A
    ]

@pytest.fixture
def football_team_ids():
    return [
        10000001644, # PSG
        10000001653, # Monaco
        10000001649, # Olympique Lyonnais
        10000000042, # Arsenal
        10000000017, # Manchester City
        10000000044, # Liverpool
        10000002829, # Real Madrid
        10000002817, # Barcelona
        10000002836, # Atletico Madrid
        10000002672, # Bayern Munich
    ]

@pytest.fixture
def raw_selection_data(football_competition_ids, football_team_ids):
    return {
        "name": "dev",
        "items": [
            {
                "name": "item-1",
                "sport": "football",
                "uid": "it-001",
                "filters": [
                    {
                        "name": "item-1-filter-1",
                        "sport": "football",
                        "uid": "it-001-filt-001",
                        "fields": {"filter_type": "empty"}
                    },
                    {
                        "name": "item-1-filter-2",
                        "sport": "football",
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
                        "sport": "football",
                        "uid": "it-001-filt-003",
                        "fields": {
                            "filter_type": "competitors",
                            "competitor_ids": football_team_ids,
                            "selection_rule": {"reference": 40, "rule": "opponent"}
                        }
                    }
                ]
            },
            {
                "name": "item-2",
                "sport": "f1",
                "uid": "it-002",
                "filters": [
                    {
                        "name": "item-2-filter-1",
                        "sport": "f1",
                        "uid": "it-002-filt-001",
                        "created_at": "2026-01-30T19:57:02",
                        "fields": {
                            "filter_type": "sessions",
                            "competition_id": 20000000040,
                            "sessions": ["Sprint", "Race"]
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

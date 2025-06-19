# import pytest

# from src.sources.api import BaseApiClient


# def test_query_api():
#     pass

# @pytest.fixture
# def mock_data():
#     return {
#         "key1": "value1",
#         "key2": "value2",
#         "key3": {
#             "subkey1": "subvalue1",
#             "subkey2": "subvalue2"
#         },
#         "key4": [
#             {"item1": "value1"},
#             {"item2": "value2", "item3": [{"subitem1": "subvalue1"}]}
#         ]
#     }

# @pytest.fixture
# def mock_paths():
#     return [
#         (["key1"], "value1"),
#         (["key2"], "value2"),
#         (["key3", "subkey1"], "subvalue1"),
#         (["key3", "subkey2"], "subvalue2"),
#         (["key4", 0, "item1"], "value1"),
#         (["key4", 1, "item2"], "value2"),
#         (["key4", 1, "item3", 0, "subitem1"], "subvalue1")
#     ]

# def test_get_value_from_json(mock_data, mock_paths):
#     api_client = BaseApiClient()
#     for path, expected_value in mock_paths:
#         value = api_client.get_value_from_json(mock_data, path)
#         assert value == expected_value, f"Expected {expected_value} for path {path}, got {value}"

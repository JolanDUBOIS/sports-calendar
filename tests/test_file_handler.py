from unittest.mock import patch

import pytest
import pandas as pd

from src.data_processing.file_io import FileHandler, CSVHandler, JSONHandler


@pytest.fixture
def csv_data():
    return pd.DataFrame({
        'id': [1, 2, 3],
        'created_at': ['2023-10-01T12:00:00', '2023-10-02T12:00:00', '2023-10-03T12:00:00'],
        'version': [1, 2, 3]
    })

@pytest.fixture
def json_data():
    return [
        {'id': 1, 'created_at': '2023-10-01T12:00:00', 'version': 1},
        {'id': 2, 'created_at': '2023-10-02T12:00:00', 'version': 2},
        {'id': 3, 'created_at': '2023-10-03T12:00:00', 'version': 3}
    ]

def test_parse_version_value():
    # Test with numeric value
    version_value, version_type = FileHandler._parse_version_value("123.456")
    assert version_value == 123.456
    assert version_type == 'numeric'

    # Test with datetime value
    version_value, version_type = FileHandler._parse_version_value("2023-10-01T12:00:00")
    assert version_value == pd.Timestamp("2023-10-01T12:00:00")
    assert version_type == 'datetime'

    # Test with invalid value
    with pytest.raises(ValueError):
        FileHandler._parse_version_value("invalid_value")

def test_write_csv(tmp_path, csv_data):
    file_path = tmp_path / "test_data_write_csv.csv"
    handler = CSVHandler(file_path)

    handler.write(csv_data)    
    written_data = pd.read_csv(file_path)
    pd.testing.assert_frame_equal(written_data, csv_data)

    handler.write(csv_data)
    written_data = pd.read_csv(file_path)
    pd.testing.assert_frame_equal(written_data, pd.concat([csv_data, csv_data], ignore_index=True))

    with patch("builtins.input", return_value="yes"):
        handler.write(csv_data, overwrite=True)
    written_data = pd.read_csv(file_path)
    pd.testing.assert_frame_equal(written_data, csv_data)

def test_write_json(tmp_path, json_data):
    file_path = tmp_path / "test_data_write_json.json"
    handler = JSONHandler(file_path)

    handler.write(json_data)
    written_data = handler._read_json(file_path)
    assert written_data == json_data
    assert file_path.stat().st_size > 0

    handler.write(json_data)
    written_data = handler._read_json(file_path)
    assert written_data == json_data + json_data

    with patch("builtins.input", return_value="yes"):
        handler.write(json_data, overwrite=True)
    written_data = handler._read_json(file_path)
    assert written_data == json_data

def test_read_csv(tmp_path, csv_data):
    file_path = tmp_path / "test_data_read_csv.csv"
    handler = CSVHandler(file_path)
    handler.write(csv_data)

    read_data = handler._read_all()
    pd.testing.assert_frame_equal(read_data, csv_data)

    read_newest_data = handler._read_newest(version_field="created_at", version_threshold="2023-10-02")
    expected_data = csv_data.iloc[1:]
    assert expected_data['id'].tolist() == read_newest_data['id'].tolist()

    read_newest_data = handler._read_newest(version_field="version", version_threshold=2)
    expected_data = csv_data.iloc[1:]
    assert expected_data['id'].tolist() == read_newest_data['id'].tolist()

def test_read_json(tmp_path, json_data):
    file_path = tmp_path / "test_data_read_json.json"
    handler = JSONHandler(file_path)
    handler.write(json_data)

    read_data = handler._read_all()
    assert read_data == json_data

    read_newest_data = handler._read_newest(version_field="created_at", version_threshold="2023-10-02")
    read_newest_data_ids = [row['id'] for row in read_newest_data]
    expected_data_ids = [row['id'] for row in json_data[1:]]
    assert read_newest_data_ids == expected_data_ids

    read_newest_data = handler._read_newest(version_field="version", version_threshold=2)
    read_newest_data_ids = [row['id'] for row in read_newest_data]
    expected_data_ids = [row['id'] for row in json_data[1:]]
    assert read_newest_data_ids == expected_data_ids

def test_delete_records_csv(tmp_path, csv_data):
    file_path = tmp_path / "test_data_delete_records_csv.csv"
    handler = CSVHandler(file_path)
    handler.write(csv_data)

    with patch("builtins.input", return_value="yes"):
        handler.delete_records(version_field="created_at", version_threshold="2023-10-02", delete_newest=False)
    read_data_ids = handler._read_all()["id"].tolist()
    expected_data_ids = csv_data.loc[1:,"id"].tolist()
    assert read_data_ids == expected_data_ids

    with patch("builtins.input", return_value="yes"):
        handler.write(csv_data, overwrite=True)
        handler.delete_records(version_field="created_at", version_threshold="2023-10-02", delete_newest=True)
    read_data_ids = handler._read_all()["id"].tolist()
    expected_data_ids = csv_data.loc[[0],"id"].tolist()
    assert read_data_ids == expected_data_ids

    with patch("builtins.input", return_value="yes"):
        handler.write(csv_data, overwrite=True)
        handler.delete_records(version_field="version", version_threshold=2, delete_newest=False)
    read_data_ids = handler._read_all()["id"].tolist()
    expected_data_ids = csv_data.loc[1:,"id"].tolist()
    assert read_data_ids == expected_data_ids

def test_delete_records_json(tmp_path, json_data):
    file_path = tmp_path / "test_data_delete_records_json.json"
    handler = JSONHandler(file_path)
    handler.write(json_data)

    with patch("builtins.input", return_value="yes"):
        handler.delete_records(version_field="created_at", version_threshold="2023-10-02", delete_newest=False)
    read_data_ids = [row['id'] for row in handler._read_all()]
    expected_data_ids = [2, 3]
    assert read_data_ids == expected_data_ids

    with patch("builtins.input", return_value="yes"):
        handler.write(json_data, overwrite=True)
        handler.delete_records(version_field="created_at", version_threshold="2023-10-02", delete_newest=True)
    read_data_ids = [row['id'] for row in handler._read_all()]
    expected_data_ids = [1]
    assert read_data_ids == expected_data_ids

    with patch("builtins.input", return_value="yes"):
        handler.write(json_data, overwrite=True)
        handler.delete_records(version_field="version", version_threshold=2, delete_newest=False)
    read_data_ids = [row['id'] for row in handler._read_all()]
    expected_data_ids = [2,3]
    assert read_data_ids == expected_data_ids

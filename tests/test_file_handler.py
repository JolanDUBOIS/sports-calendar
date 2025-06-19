from datetime import datetime, timedelta

import pytest
import pandas as pd

from src.file_io import FileHandlerFactory


@pytest.fixture
def data():
    """ Fixture to provide sample data for testing. """
    return [
        {"id": 1, "name": "Alice", "age": 30},
        {"id": 2, "name": "Bob", "age": 25},
        {"id": 3, "name": "Charlie", "age": 35}
    ]

def test_cleanup(tmp_path, data):
    """ Test the cleanup method of the file handler. """
    csv_handler = FileHandlerFactory.create_file_handler(tmp_path / "test.csv")
    json_handler = FileHandlerFactory.create_file_handler(tmp_path / "test.json")

    # Write initial data
    data_df = pd.DataFrame(data)
    csv_handler.write(data_df)
    json_handler.write(data)

    # Read data
    csv_data = csv_handler.read()
    json_data = json_handler.read()

    assert len(csv_data) == len(data)
    assert len(json_data) == len(data)

    # Check if _ctime is added
    assert "_ctime" in csv_data.columns
    assert all("_ctime" in item for item in json_data)

    # Cleanup
    cutoff_yesterday = (datetime.now() - timedelta(days=1)).isoformat(timespec='seconds')
    cutoff_tomorrow = (datetime.now() + timedelta(days=1)).isoformat(timespec='seconds')

    csv_handler.cleanup(cutoff_yesterday)
    json_handler.cleanup(cutoff_yesterday)
    
    assert len(csv_handler.read()) == len(data)
    assert len(json_handler.read()) == len(data)

    csv_handler.cleanup(cutoff_tomorrow)
    json_handler.cleanup(cutoff_tomorrow)

    assert len(csv_handler.read()) == 0
    assert len(json_handler.read()) == 0

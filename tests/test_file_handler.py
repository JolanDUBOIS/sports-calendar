import pytest
import pandas as pd

from src.data_processing.file_io.file_handler import CSVHandler, JSONHandler


def test_get_newest_version_csv():
    df = pd.DataFrame({
        'id': [1, 2, 3],
        'version': [1, 2, 3],
        'created_at': ['2023-01-01', '2023-01-02', '2023-01-02']
    })

    df_newest = CSVHandler._get_newest_version(df, 'version', 2, version_type='numeric')
    assert isinstance(df_newest, pd.DataFrame)
    assert len(df_newest) == 1
    assert df_newest['version'].values[0] == 3
    
    df_newest = CSVHandler._get_newest_version(df, 'created_at', '2023-01-01 23:00:00')
    assert isinstance(df_newest, pd.DataFrame)
    assert len(df_newest) == 2
    
    df_newest = CSVHandler._get_newest_version(df, 'version', version_type='numeric')
    assert isinstance(df_newest, pd.DataFrame)
    assert len(df_newest) == 3

    df_newest = CSVHandler._get_newest_version(df, 'created_at')
    assert isinstance(df_newest, pd.DataFrame)
    assert len(df_newest) == 3

    with pytest.raises(ValueError):
        CSVHandler._get_newest_version(df, 'non_existent_field')

    with pytest.raises(ValueError):
        CSVHandler._get_newest_version(df, 'version', version_type='non_existent_type')

def test_get_newest_version_json():
    data = [
        {"id": 1, "version": 1, "created_at": "2023-01-01"},
        {"id": 2, "version": 2, "created_at": "2023-01-02"},
        {"id": 3, "version": 3, "created_at": "2023-01-02"}
    ]

    newest_data = JSONHandler._get_newest_version(data, 'version', 2, version_type='numeric')
    assert isinstance(newest_data, list)
    assert len(newest_data) == 1
    assert newest_data[0]['version'] == 3

    newest_data = JSONHandler._get_newest_version(data, 'created_at', '2023-01-01 23:00:00')
    assert isinstance(newest_data, list)
    assert len(newest_data) == 2

    newest_data = JSONHandler._get_newest_version(data, 'version', version_type='numeric')
    assert isinstance(newest_data, list)
    assert len(newest_data) == 3

    newest_data = JSONHandler._get_newest_version(data, 'created_at')
    assert isinstance(newest_data, list)
    assert len(newest_data) == 3
    
    with pytest.raises(ValueError):
        JSONHandler._get_newest_version(data, 'non_existent_field')

    with pytest.raises(ValueError):
        JSONHandler._get_newest_version(data, 'version', version_type='non_existent_type')

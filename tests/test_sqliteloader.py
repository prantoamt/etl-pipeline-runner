# Python imports
import os
import sqlite3
import pytest

# Third-party imports
import pandas as pd
from pandas.testing import assert_frame_equal

# Self imports
from etl_pipeline_runner.services import (
    SQLiteLoader,
)
from tests.conftest import DATA_DIRECTORY


class TestSqliteLoader:
    def test_sqlite_loader_replace_if_exists_ok(self, sqlite_loader, mock_data_frame):
        sqlite_loader = sqlite_loader(if_exists=SQLiteLoader.REPLACE)
        db_path = os.path.join(DATA_DIRECTORY, sqlite_loader.db_name)
        sqlite_loader._load_to_db(data_frame=mock_data_frame)
        sqlite_loader._load_to_db(data_frame=mock_data_frame)
        connection = sqlite3.connect(db_path)
        result = pd.read_sql_query(
            f"SELECT * FROM {sqlite_loader.table_name}", connection
        )
        connection.close()
        assert_frame_equal(result, mock_data_frame)
        os.remove(db_path)

    def test_sqlite_loader_append_if_exists_ok(self, sqlite_loader, mock_data_frame):
        sqlite_loader = sqlite_loader(if_exists=SQLiteLoader.APPEND)
        appended_data_frame = pd.concat(
            [mock_data_frame, mock_data_frame],
            ignore_index=True,
        )
        db_path = os.path.join(DATA_DIRECTORY, sqlite_loader.db_name)
        sqlite_loader._load_to_db(data_frame=mock_data_frame)
        sqlite_loader._load_to_db(data_frame=mock_data_frame)
        connection = sqlite3.connect(db_path)
        result = pd.read_sql_query(
            f"SELECT * FROM {sqlite_loader.table_name}", connection
        )
        connection.close()
        assert_frame_equal(result, appended_data_frame)
        os.remove(db_path)

    def test_sqlite_loader_if_exists_fail(self, sqlite_loader, mock_data_frame):
        sqlite_loader = sqlite_loader(if_exists=SQLiteLoader.FAIL)
        db_path = os.path.join(DATA_DIRECTORY, sqlite_loader.db_name)
        sqlite_loader._load_to_db(data_frame=mock_data_frame)
        with pytest.raises(Exception):
            sqlite_loader._load_to_db(data_frame=mock_data_frame)
        os.remove(db_path)

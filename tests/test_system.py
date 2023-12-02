# Python imports
import os
import sqlite3

# Third-party imports
import pandas as pd
from pandas.testing import assert_frame_equal

# Self imports6
from src.etl_pipeline_runner.services import (
    ETLQueue,
)


class TestSystem:
    def test_kaggle_pipline_ok(self, kaggle_pipeline):
        loader = kaggle_pipeline.extractor.file_handlers[0].loader
        ETLQueue(etl_pipelines=(kaggle_pipeline,)).run()
        db_path = os.path.join(loader.output_directory, loader.db_name)
        connection = sqlite3.connect(db_path)
        result = pd.read_sql_query(f"SELECT * FROM {loader.table_name}", connection)
        connection.close()
        transformed_data = kaggle_pipeline.extractor.file_handlers[0]._data_frame
        assert_frame_equal(result, transformed_data)
        os.remove(db_path)

    def test_csv_pipline_ok(self, csv_pipeline):
        loader = csv_pipeline.extractor.file_handlers[0].loader
        ETLQueue(etl_pipelines=(csv_pipeline,)).run()
        db_path = os.path.join(loader.output_directory, loader.db_name)
        connection = sqlite3.connect(db_path)
        result = pd.read_sql_query(f"SELECT * FROM {loader.table_name}", connection)
        connection.close()
        transformed_data = csv_pipeline.extractor.file_handlers[0]._data_frame
        assert_frame_equal(result, transformed_data)
        os.remove(db_path)

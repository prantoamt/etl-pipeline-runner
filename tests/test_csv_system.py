# Python imports
import os
import sqlite3

# Third-party imports
import pandas as pd
import numpy as np
from pandas.testing import assert_frame_equal

# Self imports6
from src.etl_pipeline_runner.services import (
    ETLPipeline,
    DataExtractor,
    CSVHandler,
    SQLiteLoader,
    ETLQueue,
)

DATA_DIRECTORY = os.path.join(os.getcwd(), "data")


def transform_weather(data_frame: pd.DataFrame):
    data_frame = data_frame.drop(
        columns=[
            data_frame.columns[0],
            data_frame.columns[5],
            data_frame.columns[6],
            data_frame.columns[7],
            data_frame.columns[8],
            data_frame.columns[9],
            data_frame.columns[10],
        ],
        axis=1,
    )
    data_frame = data_frame.rename(columns={"Tsun": "tsun"})
    return data_frame


weather_loader = SQLiteLoader(
    db_name="project.sqlite",
    table_name="weather",
    if_exists=SQLiteLoader.REPLACE,
    index=False,
    method=None,
    output_directory=DATA_DIRECTORY,
)

dtype = {
    "Date": str,
    "Tavg": np.float64,
    "Tmin": np.float64,
    "Tmax": np.float64,
    "Prcp": np.float64,
    "Snow": np.float64,
    "Wdir": np.int64,
    "Wspd": np.float64,
    "Wpgt": np.float64,
    "Pres": np.float64,
    "Tsun": np.float64,
}

weather_csv_handler = CSVHandler(
    file_name="VYNT0.csv.gz",
    sep=",",
    names=[
        "Date",
        "Tavg",
        "Tmin",
        "Tmax",
        "Prcp",
        "Snow",
        "Wdir",
        "Wspd",
        "Wpgt",
        "Pres",
        "Tsun",
    ],
    compression="gzip",
    dtype=dtype,
    transformer=transform_weather,
    loader=weather_loader,
)

weather_extractor = DataExtractor(
    data_name="Weather",
    url="https://bulk.meteostat.net/v2/daily/VYNT0.csv.gz",
    type=DataExtractor.CSV,
    file_handlers=(weather_csv_handler,),
)

weather_pipeline = ETLPipeline(
    extractor=weather_extractor,
)


def test_csv_system():
    ETLQueue(etl_pipelines=(weather_pipeline,)).run()
    db_path = os.path.join(weather_loader.output_directory, weather_loader.db_name)
    connection = sqlite3.connect(db_path)
    result = pd.read_sql_query(f"SELECT * FROM {weather_loader.table_name}", connection)
    connection.close()
    data = weather_pipeline.extractor.file_handlers[0]._data_frame
    assert_frame_equal(result, data)

# Python imports
import os

# Third party imports
import pytest
import numpy as np
import pandas as pd

# Self imports
from src.etl_pipeline_runner.services import (
    ETLPipeline,
    DataExtractor,
    CSVHandler,
    SQLiteLoader,
)

DATA_DIRECTORY = os.path.join(os.getcwd(), "data")


@pytest.fixture
def kaggle_pipeline():
    songs_loader = SQLiteLoader(
        db_name="test.sqlite",
        table_name="song_lyrics",
        if_exists=SQLiteLoader.REPLACE,
        index=False,
        method=None,
        output_directory=DATA_DIRECTORY,
    )
    songs_dtype = {
        "#": "Int64",
        "artist": str,
        "seq": str,
        "song": str,
        "label": np.float64,
    }

    def transform_songs(data_frame: pd.DataFrame):
        data_frame = data_frame.drop(columns=data_frame.columns[0], axis=1)
        data_frame = data_frame.rename(columns={"seq": "lyrics"})
        return data_frame

    songs_csv_handler = CSVHandler(
        file_name="labeled_lyrics_cleaned.csv",
        sep=",",
        names=None,
        dtype=songs_dtype,
        transformer=transform_songs,
        loader=songs_loader,
    )

    songs_extractor = DataExtractor(
        data_name="Song lyrics",
        url="https://www.kaggle.com/datasets/edenbd/150k-lyrics-labeled-with-spotify-valence",
        type=DataExtractor.KAGGLE_ARCHIVE,
        file_handlers=(songs_csv_handler,),
    )

    songs_pipeline = ETLPipeline(
        extractor=songs_extractor,
    )
    yield songs_pipeline


@pytest.fixture
def csv_pipeline():
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
        db_name="test.sqlite",
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
    yield weather_pipeline


@pytest.fixture
def sqlite_loader(**kwargs):
    def _sqlite_loader(**kwargs):
        db_name = "test.sqlite"
        table_name = "loader_table"
        if_exists = kwargs.pop("if_exists")
        sqlite_loader = SQLiteLoader(
            db_name=db_name,
            table_name=table_name,
            if_exists=if_exists,
            index=None,
            method=None,
            output_directory=DATA_DIRECTORY,
        )
        return sqlite_loader

    return _sqlite_loader

@pytest.fixture
def csv_handler():
    csv_handler = CSVHandler(
        file_name="demo.csv",
        sep=",",
        names=None,
        dtype=str,
        transformer=None,
        loader=None
    )
    yield csv_handler

@pytest.fixture
def mock_data_frame():
    data_frame = pd.DataFrame(
        {
            "float": [1.5],
            "int": [1],
            "datetime": [pd.to_datetime("20180310", utc=False)],
            "string": ["foo"],
        },
    )
    data_frame = data_frame.astype({'float': np.float64, 'int': np.int64, 'datetime': str, 'string': str})
    yield data_frame

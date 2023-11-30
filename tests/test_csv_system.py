# Python imports
import os

# Third-party imports
import numpy as np
import pandas as pd

# Self imports6
from src.etl_pipeline_runner.services import (
    ETLPipeline,
    DataExtractor,
    CSVInterpreter,
    SQLiteLoader,
    ETLQueue,
)

DATA_DIRECTORY = os.path.join(os.getcwd(), "data")


def construct_songs_pipeline() -> ETLPipeline:
    weather_csv_interpreter = CSVInterpreter(
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
    )

    weather_extractor = DataExtractor(
        data_name="Weather",
        url="https://bulk.meteostat.net/v2/daily/VYNT0.csv.gz",
        type=DataExtractor.CSV,
        interpreters=(weather_csv_interpreter,),
    )

    def transform_weather(data_frame: pd.DataFrame):
        data_frame = data_frame.drop(columns=data_frame.columns[0], axis=1)
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

    weather_pipeline = ETLPipeline(
        extractor=weather_extractor,
        transformer=transform_weather,
        loader=weather_loader,
    )
    return weather_pipeline


def test_csv_system():
    weather_pipeline = construct_songs_pipeline()
    pipeline_queue = ETLQueue(etl_pipelines=(weather_pipeline,)).run()
    assert pipeline_queue == True

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
    songs_dtype = {
        "#": "Int64",
        "artist": str,
        "seq": str,
        "song": str,
        "label": np.float64,
    }

    songs_csv_interpreter = CSVInterpreter(
        file_name="labeled_lyrics_cleaned.csv",
        sep=",",
        names=None,
        dtype=songs_dtype,
    )

    songs_extractor = DataExtractor(
        data_name="Song lyrics",
        url="https://www.kaggle.com/datasets/edenbd/150k-lyrics-labeled-with-spotify-valence",
        type=DataExtractor.KAGGLE_ARCHIVE,
        interpreters=(songs_csv_interpreter,),
    )

    songs_loader = SQLiteLoader(
        db_name="project.sqlite",
        table_name="song_lyrics",
        if_exists=SQLiteLoader.REPLACE,
        index=False,
        method=None,
        output_directory=DATA_DIRECTORY,
    )

    def transform_lyrics(data_frame: pd.DataFrame):
        data_frame = data_frame.drop(columns=data_frame.columns[0], axis=1)
        data_frame = data_frame.rename(columns={"seq": "lyrics"})
        return data_frame

    songs_pipeline = ETLPipeline(
        extractor=songs_extractor,
        transformer=transform_lyrics,
        loader=songs_loader,
    )
    return songs_pipeline


def test_kaggle_system():
    songs_pipeline = construct_songs_pipeline()
    pipeline_queue = ETLQueue(etl_pipelines=(songs_pipeline,)).run()
    assert pipeline_queue == True

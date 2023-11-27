# Python imports
import os

# Third-party imports
import numpy as np
import pandas as pd

# Self imports6
from src.etl_pipeline_runner.services import (
    ETLPipeline,
    DataSource,
    CSVFile,
    SQLiteLoader,
    ETLQueue,
)

DATA_DIRECTORY = os.path.join(os.getcwd(), "data")

def construct_songs_pipeline() -> ETLPipeline:
    songs_output_db = SQLiteLoader(
        db_name="project.sqlite",
        table_name="song_lyrics",
        if_exists=SQLiteLoader.REPLACE,
        index=False,
        method=None,
        output_directory=DATA_DIRECTORY,
    )
    songs_file_dtype = {
        "#": "Int64",
        "artist": str,
        "seq": str,
        "song": str,
        "label": np.float64,
    }

    def transform_lyrics(data_frame: pd.DataFrame):
        data_frame = data_frame.drop(columns=data_frame.columns[0], axis=1)
        data_frame = data_frame.rename(columns={"seq": "lyrics"})
        return data_frame

    songs_file = CSVFile(
        file_name="labeled_lyrics_cleaned.csv",
        sep=",",
        names=None,
        dtype=songs_file_dtype,
        transform=transform_lyrics,
    )
    songs_data_source = DataSource(
        data_name="Song lyrics",
        url="https://www.kaggle.com/datasets/edenbd/150k-lyrics-labeled-with-spotify-valence",
        source_type=DataSource.KAGGLE_DATA,
        files=(songs_file,),
    )
    songs_pipeline = ETLPipeline(
        data_source=songs_data_source,
        loader=songs_output_db,
    )
    return songs_pipeline


def test_kaggle_system():
    songs_pipeline = construct_songs_pipeline()
    pipeline_queue = ETLQueue(etl_pipelines=(songs_pipeline,)).run()
    assert pipeline_queue == True
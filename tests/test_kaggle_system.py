# Python imports
import os
import sqlite3
# Third-party imports
import numpy as np
import pandas as pd
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
    
def transform_songs(data_frame: pd.DataFrame):
    data_frame = data_frame.drop(columns=data_frame.columns[0], axis=1)
    data_frame = data_frame.rename(columns={"seq": "lyrics"})
    return data_frame

songs_loader = SQLiteLoader(
    db_name="project.sqlite",
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

def test_kaggle_system():
    ETLQueue(etl_pipelines=(songs_pipeline,)).run()
    db_path = os.path.join(songs_loader.output_directory, songs_loader.db_name)
    connection = sqlite3.connect(db_path)
    result = pd.read_sql_query(f"SELECT * FROM {songs_loader.table_name}", connection)
    connection.close()
    data = songs_pipeline.extractor.file_handlers[0]._data_frame
    assert_frame_equal(result, data)

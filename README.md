## etl-pipeline-runner

Extract Transform Load (ETL) pipeline runner is a simple, yet effective python 
package to run ETL-Pipelines for Data sceince projects. The sole purpose of this
package is to:

```Extract data from a source --> Transform the data according to the necessity --> Load data to a Database```

## Installation

Install the library with pip:


```
    pip install etl-pipeline-runner
```

## Usage
### Run an ETL Pipeline that extracts data from kaggle and stores it in a SQLite Database.
----------

Data source: https://www.kaggle.com/datasets/edenbd/150k-lyrics-labeled-with-spotify-valence

Destination: Under ``songs`` table of ``project.sqlite`` Database. Suppose the database is located or wil be created in ``/data`` directory.

#### Example code:
1. Import the following services from ``etl_pipeline_runner``

```
from etl_pipeline_runner.services import (
    ETLPipeline,
    DataSource,
    CSVFile,
    SQLiteDB,
    ETLQueue,
)
```

2. Create an object of the SQLiteDB service.

```
    DATA_DIRECTORY = os.path.join(os.getcwd(), "data")
    output_db = SQLiteDB(
        db_name="project.sqlite",
        table_name="songs",
        if_exists=SQLiteDB.REPLACE,
        index=False,
        method=None,
        output_directory=DATA_DIRECTORY,
    )
```

Parameters description:

|             Parameter               |             Description                                                                                     |
|-------------------------------------|-------------------------------------------------------------------------------------------------------------|
|             db_name: str            | Name of the database.                                                                                       |
|             table_name: str         | Table name where data will be stored.                                                                       |
|             if_exists: str          | Action if the table already exists. Possible options: ``SQLiteDB.REPLACE``, ``SQLiteDB.APPEND``, ``SQLiteDB.FAIL``.|
|             index: bool             | Write DataFrame index as a column. Uses index_label as the column name in the table. (From pandas Doc).     |
|             method: Callable        | Controls the SQL insertion clause used. (From pandas doc).                                                  |
|             output_directory: str   | Path where the databse is located or wil be created.                                                        |

3. Create a function that defines the transformation you want to perform on the dataset before loading to the Database.
    The function signature must match the following. Here pd refers to pandas.

```
    def transform_songs(data_frame: pd.DataFrame):
        data_frame = data_frame.drop(columns=data_frame.columns[0], axis=1)
        data_frame = data_frame.rename(columns={"seq": "lyrics"})
        return data_frame
```

4. Create an object of the CSVFile service.

``` 
    columns_dtype = {
        "#": "Int64",
        "artist": str,
        "seq": str,
        "song": str,
        "label": np.float64,
    }

    songs_file = CSVFile(
        file_name="labeled_lyrics_cleaned.csv",
        sep=",",
        names=None,
        dtype=columns_dtype,
        transform=transform_songs,
    )
```

Parameters description:

|             Parameter               |             Description                                       |
|-------------------------------------|---------------------------------------------------------------|
|           file_name: str            | Name of the csv file. It must match with the actual filename. |
|           sep: str                  | Separetor used in the csv file.                               |
|           names: list               | Name of the columns if csv file does not contains it.         |
|           dtype: dict               | Type of the columns in the csv file.                          |
|           transform: Callable       | Function that defines the transformation on the data.         |

5. Create an object of the DataSource service.

```
    songs_data_source = DataSource(
        data_name="Song lyrics",
        url="https://www.kaggle.com/datasets/edenbd/150k-lyrics-labeled-with-spotify-valence",
        source_type=DataSource.KAGGLE_DATA,
        files=(songs_file,),
    )
```

Parameters description:

|             Parameter               |             Description                                                                               |
|-------------------------------------|-------------------------------------------------------------------------------------------------------|
|           data_name: str            | Name of the data. (Could be anything of your choice).                                                 |
|           url: str                  | Url of the data source.                                                                               |
|           source_type: str          | Type of the source. Possible options: ``DataSource.KAGGLE_DATA``, ``DataSource.DIRECT_READ``. Use ``DataSource.KAGGLE_DATA`` if the source is kaggle. Use ``DataSource.DIRECT_READ`` if the ``url`` directly downloads a csv file instead of ``.zip``. Other types of sources are not supported yet. |
|           files: Tuple(CSVFile)     | Files that exist in the data source.                                                                  |

6. Create an object of ETLPipeline.

```
    songs_pipeline = ETLPipeline(
        data_source=songs_data_source,
        sqlite_db=songs_output_db,
    )
```
Parameters description:

|             Parameter               |             Description           |
|-------------------------------------|-----------------------------------|
|           data_source: DataSource   | An object of DataSource service.  |
|           sqlite_db: SQLiteDB       | An object of SQLiteBD service.    |


7. Finally run the pipeline:

```
    if __name__ == "__main__":
        ETLQueue(etl_pipelines=(songs_pipeline,)).run()
```


## Contributing
This is an open source project and I welcome contributions. Please create an issue first and make a feature branch
assiciated to the issue.

## Local Development Setup

1. Clone the repository:

```
    git clone git@github.com:prantoamt/etl-pipeline-runner.git
```

2. Install pdm package manager based on your local environment: https://pdm-project.org/latest/

3. Go to the project directory and install the requirements using pdm:

```
    pdm install
```

4. Open up the project in VS code, make your changes and create a pull request with proper description.
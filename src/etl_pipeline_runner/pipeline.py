# Python imports
import requests
import logging
from typing import Callable, List, Iterable, Tuple, Any, Union
import shutil
import os, sys
import sqlite3
from tqdm import tqdm

# Third-party imports
import pandas as pd
import opendatasets as od

# Self imports


class SQLiteDB:
    FAIL = "fail"
    REPLACE = "replace"
    APPEND = "append"

    def __init__(
        self,
        db_name: str,
        table_name: str,
        if_exists: str,
        index: bool,
        output_directory: str,
        method: Callable[
            [pd.DataFrame, sqlite3.Connection, List, Iterable[Tuple[Any]]], None
        ] = None,
    ) -> None:
        self.db_name = db_name
        self.table_name = table_name
        self.if_exists = if_exists
        self.index = index
        self.output_directory = output_directory
        self.method = method

    def _load_to_db(self, data_frame: pd.DataFrame):
        db_path = os.path.join(self.output_directory, self.db_name)
        try:
            connection = sqlite3.connect(db_path)
            data_frame.to_sql(
                self.table_name,
                connection,
                if_exists=self.if_exists,
                index=self.index,
                method=self.method,
            )
            connection.close()
        except sqlite3.Error as e:
            logging.error(msg=f"Error while creating SQLite DB: {e}")
            sys.exit(1)


class CSVFile:
    ZIP_COMPRESSION = "zip"
    GZIP_COMPRESSION = "gzip"
    BZIP2_COMPRESSION = "bz2"
    ZSTD_COMPRESSION = "zstd"
    XZ_COMPRESSION = "xz"
    TAR_COMPRESSION = "tar"

    def __init__(
        self,
        file_name: str,
        sep: str,
        dtype: dict = None,
        names: Union[List[str], None] = None,
        transform: Callable[[pd.DataFrame], pd.DataFrame] = None,
        file_path=None,
        compression: str = None,
        encoding="utf-8",
    ) -> None:
        self.file_name = file_name
        self.sep = sep
        self.names = names
        self.dtype = dtype
        self._transform = transform
        self.file_path = file_path
        self.compression = compression
        self.encoding = encoding
        self._data_frame = None


class DataSource:
    KAGGLE_DATA = "kaggle"
    DIRECT_READ = "direct_read"

    def __init__(
        self,
        data_name: str,
        url: str,
        source_type: str,
        files: Tuple[CSVFile],
    ) -> None:
        self.data_name = data_name
        self.url = url
        self.source_type = source_type
        self.files = files
        self._validate()

    def _validate(self):
        if len(self.files) == 0:
            raise ValueError("Number of files can not be ZERO in any DataSource!")
        if self.source_type == self.DIRECT_READ and len(self.files) > 1:
            raise ValueError(
                "Number of files can not be more than 1 if the source type is direct read!"
            )

    def _chunk_download(self, url, file_name: str, chunk_size=1048576) -> None:
        try:
            with requests.get(url=url) as req:
                size = int(req.headers["Content-Length"])
                size_read = 0
                with tqdm(
                    total=size,
                    initial=size_read,
                    unit="B",
                    unit_scale=True,
                    unit_divisor=1024,
                    desc="Downloading " + os.path.basename(file_name),
                    miniters=1,
                ) as pbar:
                    with open(file=file_name, mode="wb") as f:
                        for chunk in req.iter_content(chunk_size=chunk_size):
                            if chunk:
                                f.write(chunk)
                            size_read = min(size, size_read + chunk_size)
                            pbar.update(n=size)
                            pbar.close()
        except Exception as e:
            logging.error(f"Can not download {os.path.basename(file_name)}: {e}")
            sys.exit(1)

    def _download(self, output_dir: str) -> str:
        if self.source_type == DataSource.KAGGLE_DATA:
            file_path = self._download_kaggle_zip_file(output_dir=output_dir)
        if self.source_type == DataSource.DIRECT_READ:
            file_path = self._download_direct_read_file(output_dir=output_dir)
        return file_path

    def _download_kaggle_zip_file(self, output_dir: str) -> None:
        try:
            od.download(
                dataset_id_or_url=self.url,
                data_dir=output_dir,
                force=False,
                dry_run=False,
            )
            dataset_id = od.utils.kaggle_direct.get_kaggle_dataset_id(
                dataset_id_or_url=self.url
            )
            id = dataset_id.split("/")[1]
            file_path = os.path.join(output_dir, id)
        except Exception as e:
            logging.error(msg=f"Error while downloading kaggle data: {e}")
            sys.exit(1)
        return file_path

    def _download_direct_read_file(self, output_dir: str) -> str:
        file_path = os.path.join(output_dir, self.files[0].file_name)
        if os.path.isfile(file_path):
            print("Skipping download: the file already exists!")
            return output_dir
        self._chunk_download(url=self.url, file_name=file_path)
        return output_dir


class ETLPipeline:
    def __init__(self, data_source: DataSource, sqlite_db: SQLiteDB = None) -> None:
        self.data_source = data_source
        self.sqlite_db = sqlite_db

    def _extract_data(self) -> str:
        output_dir = self.sqlite_db.output_directory if self.sqlite_db else "."
        return self.data_source._download(output_dir=output_dir)

    def _transform_data(self, file: CSVFile) -> pd.DataFrame:
        data_frame = pd.read_csv(
            filepath_or_buffer=file.file_path,
            sep=file.sep,
            header=0,
            names=file.names,
            compression=file.compression,
            dtype=file.dtype,
            encoding=file.encoding,
        )
        if file._transform:
            data_frame = file._transform(data_frame=data_frame)
        return data_frame

    def _load_data(self, file: CSVFile) -> None:
        if self.sqlite_db != None:
            self.sqlite_db._load_to_db(data_frame=file._data_frame)

    def run_pipeline(self) -> None:
        file_path = self._extract_data()
        tqdm_files = tqdm(
            iterable=self.data_source.files, total=len(self.data_source.files)
        )
        for item in tqdm_files:
            tqdm_files.set_description(desc=f"Processing {item.file_name}")
            item.file_path = os.path.join(file_path, item.file_name)
            item._data_frame = self._transform_data(file=item)
            self._load_data(file=item)
            os.remove(self.data_source.files[0].file_path)
        if self.data_source.source_type != DataSource.DIRECT_READ:
            shutil.rmtree(file_path)


class ETLQueue:
    def __init__(self, etl_pipelines: Tuple[ETLPipeline]) -> None:
        self.etl_pipelines = etl_pipelines

    def run(self) -> None:
        etl_tqdm = tqdm(self.etl_pipelines, total=len(self.etl_pipelines))
        for pipeline in etl_tqdm:
            etl_tqdm.set_description(
                desc=f"Running {pipeline.data_source.data_name} pipeline"
            )
            pipeline.run_pipeline()

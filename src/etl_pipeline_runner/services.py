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


class SQLiteLoader:
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


class CSVInterpreter:
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
        file_path=None,
        compression: str = None,
        encoding="utf-8",
    ) -> None:
        self.file_name = file_name
        self.sep = sep
        self.names = names
        self.dtype = dtype
        self.file_path = file_path
        self.compression = compression
        self.encoding = encoding
        self._data_frame = None

    def _get_data_frame(self) -> pd.DataFrame:
        data_frame = pd.read_csv(
            filepath_or_buffer=self.file_path,
            sep=self.sep,
            header=0,
            names=self.names,
            compression=self.compression,
            dtype=self.dtype,
            encoding=self.encoding,
        )
        return data_frame


class DataExtractor:
    KAGGLE_ARCHIVE = "kaggle_archive"
    CSV = "csv"

    def __init__(
        self,
        data_name: str,
        url: str,
        type: str,
        interpreters: Tuple[CSVInterpreter],
    ) -> None:
        self.data_name = data_name
        self.url = url
        self.type = type
        self.interpreters = interpreters
        self._validate()

    def _validate(self):
        if len(self.interpreters) == 0:
            raise ValueError(
                "Number of interpreters can not be ZERO in any DataExtractor!"
            )
        if self.type == self.CSV and len(self.interpreters) > 1:
            raise ValueError(
                f"Number of interpreters can not be more than 1 if the source type is {self.CSV}!"
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
        if self.type == DataExtractor.KAGGLE_ARCHIVE:
            file_path = self._download_kaggle_archive(output_dir=output_dir)
        if self.type == DataExtractor.CSV:
            file_path = self._download_CSV_file(output_dir=output_dir)
        return file_path

    def _download_kaggle_archive(self, output_dir: str) -> None:
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

    def _download_CSV_file(self, output_dir: str) -> str:
        file_path = os.path.join(output_dir, self.interpreters[0].file_name)
        if os.path.isfile(file_path):
            print("Skipping download: the file already exists!")
            return output_dir
        self._chunk_download(url=self.url, file_name=file_path)
        return output_dir


class ETLPipeline:
    def __init__(
        self,
        extractor: DataExtractor,
        transformer: Callable[[pd.DataFrame], pd.DataFrame] = None,
        loader: SQLiteLoader = None,
    ) -> None:
        self.extractor = extractor
        self.transformer = transformer
        self.loader = loader

    def _extract_data(self) -> str:
        output_dir = self.loader.output_directory if self.loader else "."
        return self.extractor._download(output_dir=output_dir)

    def _load_data(self, data_frame: pd.DataFrame) -> None:
        if self.loader != None:
            self.loader._load_to_db(data_frame=data_frame)

    def run_pipeline(self) -> None:
        file_path = self._extract_data()
        tqdm_interpreters = tqdm(
            iterable=self.extractor.interpreters,
            total=len(self.extractor.interpreters),
        )
        for item in tqdm_interpreters:
            tqdm_interpreters.set_description(desc=f"Processing {item.file_name}")
            item.file_path = os.path.join(file_path, item.file_name)
            transformed_data = (
                self.transformer(data_frame=item._get_data_frame())
                if self.transformer
                else item._get_data_frame()
            )
            self._load_data(data_frame=transformed_data)
            os.remove(self.extractor.interpreters[0].file_path)
        if self.extractor.type != DataExtractor.CSV:
            shutil.rmtree(file_path)


class ETLQueue:
    def __init__(self, etl_pipelines: Tuple[ETLPipeline]) -> None:
        self.etl_pipelines = etl_pipelines

    def run(self) -> bool:
        etl_tqdm = tqdm(self.etl_pipelines, total=len(self.etl_pipelines))
        for pipeline in etl_tqdm:
            etl_tqdm.set_description(
                desc=f"Running {pipeline.extractor.data_name} pipeline"
            )
            pipeline.run_pipeline()
        return True

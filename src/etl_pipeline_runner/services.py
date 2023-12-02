# Python imports
import requests
import logging
from typing import Callable, List, Iterable, Tuple, Any, Union
import shutil
import os, sys

# Third-party imports
from tqdm import tqdm
import pandas as pd
import sqlite3
import opendatasets as od
from sqlalchemy.types import NVARCHAR, DateTime, Float, INT

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

    def _sql_col(self, data_frame: pd.DataFrame):
        dtypedict = {}
        for i, j in zip(data_frame.columns, data_frame.dtypes):
            if "object" in str(j):
                dtypedict.update({i: str(NVARCHAR(length=255))})

            if "datetime" in str(j):
                dtypedict.update({i: str(DateTime())})

            if "float" in str(j):
                dtypedict.update({i: str(Float(precision=3, asdecimal=True))})

            if "int" in str(j):
                dtypedict.update({i: str(INT())})
        return dtypedict

    def _load_to_db(self, data_frame: pd.DataFrame):
        db_path = os.path.join(self.output_directory, self.db_name)
        try:
            dtypes = self._sql_col(data_frame)
            connection = sqlite3.connect(db_path)
            data_frame.to_sql(
                self.table_name,
                con=connection,
                if_exists=self.if_exists,
                index=self.index,
                dtype=dtypes,
                method=self.method,
            )
        except sqlite3.Error as e:
            logging.error(msg=f"Error while creating SQLite DB: {e}")
            sys.exit(1)
        finally:
            connection.close()


class CSVHandler:
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
        loader: SQLiteLoader,
        transformer: Callable[[pd.DataFrame], pd.DataFrame] = None,
        dtype: dict = None,
        names: Union[List[str], None] = None,
        compression: str = None,
        encoding="utf-8",
    ) -> None:
        self.file_name = file_name
        self.sep = sep
        self.loader = loader
        self.transformer = transformer
        self.names = names
        self.dtype = dtype
        self.file_path = None
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
        file_handlers: Tuple[CSVHandler],
    ) -> None:
        self.data_name = data_name
        self.url = url
        self.type = type
        self.file_handlers = file_handlers
        self._validate()

    def _validate(self):
        if len(self.file_handlers) == 0:
            raise ValueError(
                "Number of file_handlers can not be ZERO in any type of DataExtractor!"
            )
        if self.type == self.CSV and len(self.file_handlers) > 1:
            raise ValueError(
                f"Number of file_handlers can not be more than 1 if the source type is {self.CSV}!"
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
        file_path = os.path.join(output_dir, self.file_handlers[0].file_name)
        if os.path.isfile(file_path):
            print("Skipping download: the file already exists!")
            return output_dir
        self._chunk_download(url=self.url, file_name=file_path)
        return output_dir


class ETLPipeline:
    def __init__(
        self,
        extractor: DataExtractor,
    ) -> None:
        self.extractor = extractor

    def _extract_data(self) -> str:
        output_dir = os.path.join(os.getcwd(), "data")
        return self.extractor._download(output_dir=output_dir)

    def run_pipeline(self) -> None:
        file_path = self._extract_data()
        tqdm_file_handlers = tqdm(
            iterable=self.extractor.file_handlers,
            total=len(self.extractor.file_handlers),
        )
        for item in tqdm_file_handlers:
            tqdm_file_handlers.set_description(desc=f"Processing {item.file_name}")
            item.file_path = os.path.join(file_path, item.file_name)
            item._data_frame = (
                item.transformer(data_frame=item._get_data_frame())
                if item.transformer
                else item._get_data_frame()
            )
            item.loader._load_to_db(data_frame=item._data_frame)
            os.remove(self.extractor.file_handlers[0].file_path)
        if self.extractor.type != DataExtractor.CSV:
            shutil.rmtree(file_path)


class ETLQueue:
    def __init__(self, etl_pipelines: Tuple[ETLPipeline]) -> None:
        self.etl_pipelines = etl_pipelines

    def run(self) -> None:
        etl_tqdm = tqdm(self.etl_pipelines, total=len(self.etl_pipelines))
        for pipeline in etl_tqdm:
            etl_tqdm.set_description(
                desc=f"Running {pipeline.extractor.data_name} pipeline"
            )
            pipeline.run_pipeline()

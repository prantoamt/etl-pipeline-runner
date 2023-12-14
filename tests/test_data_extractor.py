# Python imports
import os, shutil

# Third-party imports

# Self imports
from tests.conftest import DATA_DIRECTORY


class TestDataExtractor:
    def test_csv_extractor_ok(self, csv_data_extractor):
        csv_data_extractor._download(output_dir=DATA_DIRECTORY)
        file_path = os.path.join(DATA_DIRECTORY, csv_data_extractor.file_handlers[0].file_name)
        assert os.path.exists(file_path) == True
        os.remove(file_path)

    def test_kaggle_archive_extractor_ok(self, kaggle_archive_data_extractor):
        file_path=kaggle_archive_data_extractor._download(output_dir=DATA_DIRECTORY)
        assert os.path.exists(file_path) == True
        shutil.rmtree(file_path)
        
    
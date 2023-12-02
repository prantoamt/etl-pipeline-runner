# Python imports

# Third-party imports
from pandas.testing import assert_frame_equal

# Self imports6
from etl_pipeline_runner.services import (
    CSVHandler,
)


class TestCSVHandler:
    def test_transform_ok(csv_handler, mock_data_frame):
        expected_data = mock_data_frame.copy()
        expected_data = expected_data.drop(columns=mock_data_frame.columns[0], axis=1)
        expected_data = expected_data.rename(columns={"int": "integer"})

        def transform(data_frame):
            data_frame = data_frame.drop(columns=data_frame.columns[0], axis=1)
            data_frame = data_frame.rename(columns={"int": "integer"})
            return data_frame

        csv_handler.transformer = transform
        transformed_data = csv_handler.transformer(mock_data_frame)
        assert_frame_equal(transformed_data, expected_data)

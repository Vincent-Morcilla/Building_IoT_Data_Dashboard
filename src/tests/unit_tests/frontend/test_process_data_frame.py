import pytest
import pandas as pd
from components.plot_generator import process_data_frame


# Sample DataFrame for testing
sample_data_frame = pd.DataFrame(
    {
        "Category": ["A", "B", "C", "A", "B", "C"],
        "Subcategory": ["X", "Y", "Z", "X", "Y", "Z"],
        "Value": [10, 15, 7, 12, 18, 5],
    }
)


def test_process_data_frame_filter_groupby_aggregate():
    """Test filtering, grouping, and aggregation functionality of process_data_frame."""

    data_processing_config = {
        "filter": {"Category": "A"},
        "groupby": ["Subcategory"],
        "aggregation": {"TotalValue": ("Value", "sum")},
    }
    expected_data_frame = pd.DataFrame(
        {"Subcategory": ["X"], "TotalValue": [22]}  # Sum of 10 and 12 for Category 'A'
    )

    processed_data_frame = process_data_frame(sample_data_frame, data_processing_config)

    pd.testing.assert_frame_equal(processed_data_frame, expected_data_frame)


def test_process_data_frame_no_matching_filter():
    """Test process_data_frame with a filter that results in an empty DataFrame."""

    data_processing_config = {
        "filter": {"Category": "D"},  # No matching category
        "groupby": ["Subcategory"],
        "aggregation": {"TotalValue": ("Value", "sum")},
    }

    expected_data_frame = pd.DataFrame(
        columns=["Category", "Subcategory", "Value"]
    ).astype({"Category": "object", "Subcategory": "object", "Value": "int64"})

    processed_data_frame = process_data_frame(sample_data_frame, data_processing_config)

    pd.testing.assert_frame_equal(processed_data_frame, expected_data_frame)


def test_process_data_frame_invalid_column_in_filter():
    """Test process_data_frame raises KeyError for a filter on a non-existent column."""

    data_processing_config = {
        "filter": {"NonExistentColumn": "A"},
        "groupby": ["Subcategory"],
        "aggregation": {"TotalValue": ("Value", "sum")},
    }

    with pytest.raises(
        KeyError, match="Column 'NonExistentColumn' not found in DataFrame"
    ):
        process_data_frame(sample_data_frame, data_processing_config)


def test_process_data_frame_explode():
    """Test the explode transformation in process_data_frame."""

    data_frame_with_lists = pd.DataFrame(
        {
            "Category": ["A", "A"],
            "ValuesList": [[1, 2], [3, 4]],
        }
    )

    data_processing_config = {
        "transformations": [{"type": "explode", "columns": ["ValuesList"]}],
    }
    expected_data_frame = pd.DataFrame(
        {
            "Category": ["A", "A", "A", "A"],
            "ValuesList": [1, 2, 3, 4],
        }
    )

    processed_data_frame = process_data_frame(
        data_frame_with_lists, data_processing_config
    )

    # Ensure `ValuesList` is set to the same dtype as in the processed output
    expected_data_frame["ValuesList"] = expected_data_frame["ValuesList"].astype(object)
    pd.testing.assert_frame_equal(processed_data_frame, expected_data_frame)


def test_process_data_frame_invalid_column_in_explode():
    """Test process_data_frame raises ValueError when exploding a non-list column."""

    data_processing_config = {
        "transformations": [{"type": "explode", "columns": ["Category"]}],
    }

    with pytest.raises(
        ValueError,
        match="Column 'Category' cannot be exploded. Ensure it contains lists.",
    ):
        process_data_frame(sample_data_frame, data_processing_config)

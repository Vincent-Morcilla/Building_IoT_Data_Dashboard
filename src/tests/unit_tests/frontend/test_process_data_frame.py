import pytest
import pandas as pd
from components.plot_generator import process_data_frame

# Sample DataFrame for testing
data_frame = pd.DataFrame(
    {
        "Category": ["A", "B", "C", "A", "B", "C"],
        "Subcategory": ["X", "Y", "Z", "X", "Y", "Z"],
        "Value": [10, 15, 7, 12, 18, 5],
    }
)

# Configuration dictionary for data processing
data_processing_config = {
    "filter": {"Category": "A"},
    "groupby": ["Subcategory"],
    "aggregation": {"TotalValue": ("Value", "sum")},
}

# Process the DataFrame
processed_data_frame = process_data_frame(data_frame, data_processing_config)

# Expected DataFrame should have aggregated values for Category 'A'
expected_data_frame = pd.DataFrame(
    {"Subcategory": ["X"], "TotalValue": [22]}  # 10 + 12
)


def test_process_data_frame():
    """
    Test that process_data_frame correctly filters, groups, and aggregates the DataFrame.
    """
    # Assert the processed DataFrame matches the expected DataFrame
    pd.testing.assert_frame_equal(processed_data_frame, expected_data_frame)

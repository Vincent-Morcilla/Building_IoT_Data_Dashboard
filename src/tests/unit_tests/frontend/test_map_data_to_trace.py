import pytest
import pandas as pd
from components.plot_generator import process_data_frame, map_data_to_trace


# Sample data for testing
sample_data = pd.DataFrame(
    {
        "Category": ["A", "B", "C", "A", "B", "C"],
        "Subcategory": ["X", "Y", "Z", "X", "Y", "Z"],
        "Value": [10, 15, 7, 12, 18, 5],
    }
)


@pytest.mark.parametrize(
    "data_processing, data_mappings, expected_trace_kwargs",
    [
        # Positive Test Case 1: Filtering, grouping, and summing values
        (
            {
                "filter": {"Category": "A"},
                "groupby": ["Subcategory"],
                "aggregation": {"TotalValue": ("Value", "sum")},
            },
            {"x": "Subcategory", "y": "TotalValue", "name": "Subcategory"},
            {"x": ["X"], "y": [22], "name": ["X"]},
        ),
        # Positive Test Case 2: No filtering or aggregation, simple column mapping
        (
            {},
            {"x": "Category", "y": "Value", "name": "Subcategory"},
            {
                "x": ["A", "B", "C", "A", "B", "C"],
                "y": [10, 15, 7, 12, 18, 5],
                "name": ["X", "Y", "Z", "X", "Y", "Z"],
            },
        ),
        # Positive Test Case 3: Using default values if column is missing
        (
            {},
            {"x": "Category", "y": "NonExistentColumn", "name": "StaticName"},
            {
                "x": ["A", "B", "C", "A", "B", "C"],
                "y": "NonExistentColumn",
                "name": "StaticName",
            },
        ),
        # Negative Test Case 1: Empty DataFrame
        (
            {
                "filter": {"Category": "NonExistent"}
            },  # Filtering results in an empty DataFrame
            {"x": "Category", "y": "Value", "name": "Subcategory"},
            {
                "x": [],
                "y": [],
                "name": [],
            },  # Updated expectation: empty lists for trace args
        ),
        # Negative Test Case 2: Non-dictionary `data_mappings`
        (
            {},
            None,  # This should raise a ValueError due to the type check
            ValueError,
        ),
        # Negative Test Case 3: Column in data_mappings does not exist in DataFrame
        (
            {},
            {"x": "NonExistentColumn", "y": "NonExistentColumn"},
            {"x": "NonExistentColumn", "y": "NonExistentColumn"},
        ),
    ],
)
def test_map_data_to_trace(data_processing, data_mappings, expected_trace_kwargs):
    """
    Test the `map_data_to_trace` function with various configurations.

    This test verifies that `map_data_to_trace` correctly generates trace keyword arguments
    for plotting based on provided data mappings and processed data frames. It includes positive
    and negative test cases to ensure robust error handling and functionality.

    Parameters:
        data_processing (dict): Configuration for filtering, grouping, and aggregating the data.
        data_mappings (dict): Mappings of trace arguments to data frame columns or static values.
        expected_trace_kwargs (dict): Expected output dictionary of trace keyword arguments.

    Raises:
        AssertionError: If the generated trace keyword arguments do not match the expected values.
        ValueError: If `data_mappings` is not a dictionary, an error is expected.
    """
    # Process the data frame if any processing is specified
    df_processed = process_data_frame(sample_data, data_processing)

    if isinstance(expected_trace_kwargs, type) and issubclass(
        expected_trace_kwargs, Exception
    ):
        # If an error type is expected, check that it raises the correct exception
        with pytest.raises(expected_trace_kwargs):
            map_data_to_trace(df_processed, data_mappings)
    else:
        # Generate trace keyword arguments for plotting
        trace_kwargs = map_data_to_trace(df_processed, data_mappings)

        # Assert that trace_kwargs matches expected values
        assert (
            trace_kwargs == expected_trace_kwargs
        ), f"Expected {expected_trace_kwargs}, got {trace_kwargs}"

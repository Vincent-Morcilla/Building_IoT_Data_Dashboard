import pandas as pd
from components.plot_generator import process_data_frame, map_data_to_trace


# Sample data for testing
df = pd.DataFrame({
    'Category': ['A', 'B', 'C', 'A', 'B', 'C'],
    'Subcategory': ['X', 'Y', 'Z', 'X', 'Y', 'Z'],
    'Value': [10, 15, 7, 12, 18, 5]
})

# Configuration for data processing
data_processing = {
    'filter': {'Category': 'A'},
    'groupby': ['Subcategory'],
    'aggregation': {'TotalValue': ('Value', 'sum')}
}

# Process the DataFrame with the specified configuration
df_processed = process_data_frame(df, data_processing)

# Configuration for data mapping
data_mappings = {
    'x': 'Subcategory',
    'y': 'TotalValue',
    'name': 'Subcategory'
}

# Generate trace keyword arguments for plotting
trace_kwargs = map_data_to_trace(df_processed, data_mappings)

# Expected result for the trace_kwargs output
expected_trace_kwargs = {
    'x': ['X'],
    'y': [22],
    'name': ['X']
}

def test_map_data_to_trace():
    """
    Test the `map_data_to_trace` function.

    This test checks if the trace keyword arguments generated by
    `map_data_to_trace` match the expected values.
    """
    # Assert that trace_kwargs matches expected values
    assert trace_kwargs == expected_trace_kwargs, (
        f"Expected {expected_trace_kwargs}, got {trace_kwargs}"
    )

import pandas as pd
import plotly.graph_objects as go
from components.plot_generator import process_data_frame, create_traces

# Sample data for the DataFrame
df = pd.DataFrame(
    {
        "Category": ["A", "B", "C", "A", "B", "C"],
        "Subcategory": ["X", "Y", "Z", "X", "Y", "Z"],
        "Value": [10, 15, 7, 12, 18, 5],
    }
)

# Define data processing configuration
data_processing = {
    "filter": {"Category": "A"},
    "groupby": ["Subcategory"],
    "aggregation": {"TotalValue": ("Value", "sum")},
}

# Process the DataFrame
df_processed = process_data_frame(df, data_processing)

# Component configuration for trace creation
component = {
    "trace_type": "Bar",
    "data_processing": {
        "data_mappings": {
            "x": "Subcategory",
            "y": "TotalValue",
        },
        "trace_kwargs": {
            "name": "Total Value by Subcategory",
        },
    },
}

# Generate traces
traces = create_traces(df_processed, component)


def test_create_traces():
    """Test that `create_traces` returns a list with a single Bar trace."""
    assert isinstance(traces, list), "Expected traces to be a list"
    assert len(traces) == 1, "Expected one trace in the list"
    assert isinstance(traces[0], go.Bar), "Expected trace to be a Bar instance"

import pytest
import pandas as pd
import plotly.graph_objects as go
from components.plot_generator import process_data_frame, create_traces


def test_create_traces_returns_list():
    """Test that `create_traces` returns a list."""
    df_processed, component = setup_test_data()
    traces = create_traces(df_processed, component)
    assert isinstance(traces, list), "Expected traces to be a list"


def test_create_traces_single_trace():
    """Test that `create_traces` returns a list with a single trace."""
    df_processed, component = setup_test_data()
    traces = create_traces(df_processed, component)
    assert len(traces) == 1, "Expected one trace in the list"


def test_create_traces_trace_type():
    """Test that `create_traces` returns a Bar trace."""
    df_processed, component = setup_test_data()
    traces = create_traces(df_processed, component)
    assert isinstance(traces[0], go.Bar), "Expected trace to be a Bar instance"


def test_create_traces_trace_properties():
    """Test that `create_traces` generates a trace with correct properties."""
    df_processed, component = setup_test_data()
    traces = create_traces(df_processed, component)
    trace = traces[0]
    assert trace.name == "Total Value by Subcategory", "Trace name mismatch"
    assert list(trace.x) == ["X"], "Trace x values do not match expected data"
    assert list(trace.y) == [22], "Trace y values do not match expected data"


def test_create_traces_marker_configured():
    """Test that `create_traces` includes a marker property in the trace."""
    df_processed, component = setup_test_data()
    traces = create_traces(df_processed, component)
    trace = traces[0]
    assert trace.marker is not None, "Trace should have marker property configured"


def setup_test_data():
    """
    Set up sample data for testing `create_traces`.

    Returns:
        tuple: A tuple containing the processed DataFrame and component configuration.
    """
    df = pd.DataFrame(
        {
            "Category": ["A", "B", "C", "A", "B", "C"],
            "Subcategory": ["X", "Y", "Z", "X", "Y", "Z"],
            "Value": [10, 15, 7, 12, 18, 5],
        }
    )

    data_processing = {
        "filter": {"Category": "A"},
        "groupby": ["Subcategory"],
        "aggregation": {"TotalValue": ("Value", "sum")},
    }

    df_processed = process_data_frame(df, data_processing)

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

    return df_processed, component


def test_create_traces_empty_dataframe():
    """Test that `create_traces` returns an empty list for an empty DataFrame."""
    empty_df = pd.DataFrame()  # Empty DataFrame
    component = {
        "trace_type": "Bar",
        "data_processing": {"data_mappings": {"x": "Subcategory", "y": "TotalValue"}},
    }
    traces = create_traces(empty_df, component)
    assert traces == [], "Expected no traces for an empty DataFrame"


def test_create_traces_missing_trace_type():
    """Test that `create_traces` raises ValueError if `trace_type` is not specified."""
    df_processed, component = setup_test_data()
    del component["trace_type"]  # Remove `trace_type`
    with pytest.raises(
        ValueError, match="For 'go' plots, 'trace_type' must be specified."
    ):
        create_traces(df_processed, component)


def test_create_traces_missing_columns():
    """Test that `create_traces` warns about missing columns."""
    df_processed, component = setup_test_data()
    component["data_processing"]["data_mappings"][
        "y"
    ] = "NonExistentColumn"  # Introduce a missing column
    traces = create_traces(df_processed, component)
    assert traces == [], "Expected no traces due to missing columns in the DataFrame"


def test_create_traces_missing_split_by_column():
    """Test that `create_traces` warns when `split_by` column is missing."""
    df_processed, component = setup_test_data()
    component["data_processing"][
        "split_by"
    ] = "NonExistentSplitColumn"  # Set a non-existent split_by column
    traces = create_traces(df_processed, component)
    assert (
        traces == []
    ), "Expected no traces due to missing `split_by` column in the DataFrame"


def test_create_traces_with_split_by():
    """Test that `create_traces` correctly splits data when `split_by` is specified."""
    df_processed, component = setup_test_data()
    # Add a valid `split_by` that exists in the DataFrame
    component["data_processing"]["split_by"] = "Subcategory"
    traces = create_traces(df_processed, component)

    # Check that the number of traces matches the number of unique values in 'Subcategory'
    unique_subcategories = df_processed["Subcategory"].unique()
    assert len(traces) == len(
        unique_subcategories
    ), "Number of traces should match unique subcategories"

    # Optionally, verify properties of each trace
    for trace in traces:
        assert (
            trace.name in unique_subcategories
        ), "Trace name should be one of the subcategories"

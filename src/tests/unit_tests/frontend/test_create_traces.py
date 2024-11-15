import pytest
import pandas as pd
import plotly.graph_objects as go
from components.analytics import process_data_frame, create_traces


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
    del component["trace_type"]  # Remove trace_type
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
    """Test that `create_traces` warns when split_by column is missing."""
    df_processed, component = setup_test_data()
    component["data_processing"][
        "split_by"
    ] = "NonExistentSplitColumn"  # Set a non-existent split_by column
    traces = create_traces(df_processed, component)
    assert (
        traces == []
    ), "Expected no traces due to missing `split_by` column in the DataFrame"


def test_create_traces_with_split_by():
    """Test that `create_traces` correctly handles the `split_by` parameter."""
    df = pd.DataFrame(
        {
            "Category": ["A", "A", "B", "B"],
            "Subcategory": ["X", "Y", "X", "Y"],
            "Value": [10, 20, 30, 40],
        }
    )

    data_processing = {
        "data_mappings": {
            "x": "Subcategory",
            "y": "Value",
        },
        "split_by": "Category",
        "trace_kwargs": {},
    }

    component = {
        "trace_type": "Bar",
        "data_processing": data_processing,
    }

    traces = create_traces(df, component)
    assert len(traces) == 2, "Expected two traces due to `split_by` 'Category'"
    trace_names = [trace.name for trace in traces]
    assert (
        "A" in trace_names and "B" in trace_names
    ), "Trace names should correspond to `split_by` values"
    for trace in traces:
        assert isinstance(trace, go.Bar), "Each trace should be an instance of go.Bar"
        assert trace.name in ["A", "B"], "Trace names should be 'A' or 'B'"
        assert list(trace.x) == ["X", "Y"], "Trace x values should match Subcategory"
        expected_y = [10, 20] if trace.name == "A" else [30, 40]
        assert (
            list(trace.y) == expected_y
        ), f"Trace y values do not match expected data for Category {trace.name}"

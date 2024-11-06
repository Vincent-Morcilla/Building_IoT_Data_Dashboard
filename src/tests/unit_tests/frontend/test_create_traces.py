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

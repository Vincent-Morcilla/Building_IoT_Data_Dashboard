import pytest
import pandas as pd
import plotly.graph_objects as go
from components.plot_generator import create_go_figure


# Sample data for testing
df = pd.DataFrame(
    {"Category": ["A", "B", "C", "A", "B", "C"], "Value": [10, 15, 7, 12, 18, 5]}
)

# Configuration for the plot component
component = {
    "trace_type": "Bar",
    "data_mappings": {
        "x": "Category",
        "y": "Value",
    },
    "layout_kwargs": {"title": "Test Bar Plot"},
}

data_processing = {
    "trace_kwargs": {
        "name": "Value by Category",
    },
}

kwargs = {}  # Additional kwargs for go.Figure()


def test_create_go_figure_returns_figure():
    """Test that create_go_figure returns a Plotly Figure instance."""
    fig = create_go_figure(df, data_processing, component, kwargs)
    assert isinstance(fig, go.Figure), "The result should be a Plotly Figure instance"


def test_create_go_figure_correct_trace_type():
    """Test that the created figure has the correct trace type based on the component config."""
    fig = create_go_figure(df, data_processing, component, kwargs)
    assert len(fig.data) == 1, "Figure should contain one trace"
    assert isinstance(fig.data[0], go.Bar), "Trace should be of type Bar"


def test_create_go_figure_layout_config():
    """Test that the figure layout is updated based on component's layout_kwargs."""
    fig = create_go_figure(df, data_processing, component, kwargs)
    assert (
        fig.layout.title.text == "Test Bar Plot"
    ), "Layout title should match layout_kwargs"


def test_create_go_figure_data_mappings():
    """Test that data mappings are correctly applied to the figure."""
    fig = create_go_figure(df, data_processing, component, kwargs)
    x_values = fig.data[0].x
    y_values = fig.data[0].y
    assert list(x_values) == [
        "A",
        "B",
        "C",
        "A",
        "B",
        "C",
    ], "X values should match 'Category' column"
    assert list(y_values) == [
        10,
        15,
        7,
        12,
        18,
        5,
    ], "Y values should match 'Value' column"


def test_create_go_figure_invalid_trace_type():
    """Test that an invalid trace type in the component raises a ValueError."""
    invalid_component = component.copy()
    invalid_component["trace_type"] = "InvalidTrace"
    with pytest.raises(ValueError, match="Invalid trace type 'InvalidTrace' specified"):
        create_go_figure(df, data_processing, invalid_component, kwargs)


def test_create_go_figure_missing_trace_type():
    """Test that a missing trace type in the component raises a ValueError."""
    missing_trace_component = component.copy()
    missing_trace_component.pop("trace_type", None)
    with pytest.raises(
        ValueError, match="Trace type is required in 'component' to create the figure"
    ):
        create_go_figure(df, data_processing, missing_trace_component, kwargs)

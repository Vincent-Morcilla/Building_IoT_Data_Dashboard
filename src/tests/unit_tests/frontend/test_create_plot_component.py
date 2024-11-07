import pandas as pd
from dash import dcc
from components.plot_generator import create_plot_component, process_data_frame


# Test DataFrame with a list column for data processing example
df = pd.DataFrame({"Category": ["A", "B"], "Value": [[10, 12], [15, 18]]})

# Configuration for a Plotly Express bar chart with explode transformation
component_config = {
    "id": "test-plot-px-explode",
    "type": "plot",
    "library": "px",
    "function": "bar",
    "kwargs": {
        "x": "Category",
        "y": "Value",
        "data_frame": df,
    },
    "data_processing": {
        "transformations": [
            {
                "type": "explode",
                "columns": ["Value"],
            },
        ],
    },
    "layout_kwargs": {"title": "Test Bar Chart with Explode"},
    "css": {"width": "50%", "display": "inline-block"},
}


def test_create_plot_component_with_explode():
    """Test creating a plot component with explode transformation."""
    # Process DataFrame explicitly in test to verify transformation
    transformed_df = process_data_frame(df, component_config["data_processing"])
    print(transformed_df)  # Temporary print statement to debug the transformation
    assert len(transformed_df) == 4  # Check if DataFrame is exploded correctly

    # Now create the plot component with the transformed DataFrame
    plot_component = create_plot_component(component_config)
    assert isinstance(plot_component, dcc.Graph)
    fig = plot_component.figure

    # Verify that the data has been exploded correctly in the plot
    assert len(fig.data[0].x) == 4  # 2 categories * 2 values each after exploding


# Test without data processing
def test_create_plot_component_without_data_processing():
    """Test creating a plot component without any data processing."""
    # DataFrame without list column
    df_simple = pd.DataFrame(
        {"Category": ["A", "B", "C", "A", "B", "C"], "Value": [10, 15, 7, 12, 18, 5]}
    )

    component_config_simple = {
        "id": "test-plot-px",
        "type": "plot",
        "library": "px",
        "function": "bar",
        "kwargs": {
            "x": "Category",
            "y": "Value",
            "color": "Category",
            "data_frame": df_simple,
        },
        "layout_kwargs": {"title": "Test Bar Chart"},
        "css": {"width": "50%", "display": "inline-block"},
    }
    plot_component = create_plot_component(component_config_simple)
    assert isinstance(plot_component, dcc.Graph)


# Sample DataFrame for testing Plotly GO component
df_go = pd.DataFrame(
    {"Category": ["A", "B", "C", "A", "B", "C"], "Value": [10, 15, 7, 12, 18, 5]}
)

# Configuration for a Plotly GO scatter plot with trace configuration
component_config_go = {
    "id": "test-plot-go",
    "type": "plot",
    "library": "go",
    "trace_type": "Scatter",
    "data_frame": df_go,
    "data_processing": {
        "data_mappings": {
            "x": "Category",
            "y": "Value",
        },
        "trace_kwargs": {
            "mode": "markers",
        },
    },
    "layout_kwargs": {"title": "Test Scatter Plot"},
}


def test_create_plot_component_go():
    """Test creating a Plotly GO plot component with trace configuration."""
    plot_component_go = create_plot_component(component_config_go)
    assert isinstance(plot_component_go, dcc.Graph)

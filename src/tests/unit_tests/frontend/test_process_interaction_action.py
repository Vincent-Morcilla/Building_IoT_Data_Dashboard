import pytest
import pandas as pd
from plotly.graph_objects import Figure
from actions.process_interaction import process_interaction_action
from sampledata.plot_configs import sample_plot_configs as plot_configs


@pytest.fixture
def sample_source_data():
    """Provide sample source data for testing."""
    return pd.DataFrame(
        {
            "Measurement": ["Quality A", "Quality B", "Quality A", "Quality C"],
            "Value": [60, 70, 65, 80],
        }
    )


@pytest.fixture
def sample_plot_configs(sample_source_data):
    """Mock plot configuration with a box plot component for testing."""
    plot_configs["test-key"] = {
        "components": [
            {
                "id": "consumption-quality-box-plot",
                "type": "plot",
                "library": "px",
                "function": "box",
                "kwargs": {
                    "data_frame": sample_source_data,
                    "x": "Measurement",
                    "y": "Value",
                },
                "layout_kwargs": {},
                "css": {},
            }
        ]
    }
    return plot_configs


@pytest.fixture
def default_outputs():
    """Provide default output structure for testing."""
    return [
        {"component_id": "consumption-quality-box-plot", "component_property": "figure"}
    ]


@pytest.fixture
def default_interaction():
    """Define a default interaction configuration for testing."""
    return {
        "action": "process_interaction",
        "data_mapping": {
            "from": "consumption-quality-box-plot",
            "to": "consumption-quality-box-plot",
        },
        "data_processing": {"filter": {"Measurement": {"in": "selected_measurements"}}},
    }


@pytest.fixture
def default_triggers():
    """Provide default trigger configuration."""
    return [{"input_key": "selected_measurements"}]


def test_output_structure(
    sample_plot_configs, default_outputs, default_interaction, default_triggers
):
    """Test if `process_interaction_action` returns the correct output structure."""
    input_values = [["Quality A", "Quality C"]]
    output_results = process_interaction_action(
        sample_plot_configs,
        input_values,
        default_outputs,
        default_interaction,
        triggers=default_triggers,
    )

    assert len(output_results) == 1, "Expected one figure output"
    assert isinstance(
        output_results[0], Figure
    ), "Output should be a Plotly Figure instance"


def test_filtered_data_in_output(
    sample_plot_configs,
    default_outputs,
    default_interaction,
    default_triggers,
    sample_source_data,
):
    """
    Test if `process_interaction_action` correctly filters data based on input values
    and returns expected categories in the plot.
    """
    input_values = [["Quality A", "Quality C"]]
    expected_measurements = {"Quality A", "Quality C"}

    output_results = process_interaction_action(
        sample_plot_configs,
        input_values,
        default_outputs,
        default_interaction,
        triggers=default_triggers,
    )

    fig = output_results[0]
    categories_in_plot = {trace.name for trace in fig.data}

    assert (
        categories_in_plot == expected_measurements
    ), "Categories in plot do not match expected filtered data categories"


@pytest.mark.parametrize(
    "input_values, expected_categories",
    [
        ([["Quality B"]], {"Quality B"}),  # Test for single category filtering
        (
            [["Quality A", "Quality C"]],
            {"Quality A", "Quality C"},
        ),  # Test for multiple categories
        ([[]], set()),  # Test empty input results in no categories in plot
    ],
)
def test_various_input_values(
    sample_plot_configs,
    default_outputs,
    default_interaction,
    default_triggers,
    input_values,
    expected_categories,
):
    """
    Test `process_interaction_action` with various input values to ensure the figure
    output has correct filtered categories.
    """
    output_results = process_interaction_action(
        sample_plot_configs,
        input_values,
        default_outputs,
        default_interaction,
        triggers=default_triggers,
    )

    fig = output_results[0]
    categories_in_plot = {trace.name for trace in fig.data}

    assert (
        categories_in_plot == expected_categories
    ), f"Expected categories {expected_categories} but got {categories_in_plot}"

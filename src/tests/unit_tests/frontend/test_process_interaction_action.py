import pytest
import copy
import pandas as pd
from plotly.graph_objects import Figure
from dash import no_update
from actions.process_interaction import process_interaction_action
from helpers.data_processing import apply_generic_filters, apply_transformation


# Ensure these functions are used in the process_interaction_action module
import actions.process_interaction

actions.process_interaction.apply_generic_filters = apply_generic_filters
actions.process_interaction.apply_transformation = apply_transformation


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
    plot_configs = {
        "test-key": {
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
    plot_configs = copy.deepcopy(sample_plot_configs)
    input_values = [["Quality A", "Quality C"]]
    output_results = process_interaction_action(
        plot_configs,
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
    plot_configs = copy.deepcopy(sample_plot_configs)
    input_values = [["Quality A", "Quality C"]]
    expected_measurements = {"Quality A", "Quality C"}

    output_results = process_interaction_action(
        plot_configs,
        input_values,
        default_outputs,
        default_interaction,
        triggers=default_triggers,
    )

    fig = output_results[0]
    categories_in_plot = set(fig.data[0].x)

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
    plot_configs = copy.deepcopy(sample_plot_configs)
    output_results = process_interaction_action(
        plot_configs,
        input_values,
        default_outputs,
        default_interaction,
        triggers=default_triggers,
    )

    fig = output_results[0]
    if len(fig.data) > 0:
        categories_in_plot = set(fig.data[0].x)
    else:
        categories_in_plot = set()

    assert (
        categories_in_plot == expected_categories
    ), f"Expected categories {expected_categories} but got {categories_in_plot}"


def test_component_not_found(
    sample_plot_configs, default_outputs, default_interaction, default_triggers
):
    """Test handling when a component ID is not found."""
    plot_configs = copy.deepcopy(sample_plot_configs)
    invalid_interaction = copy.deepcopy(default_interaction)
    invalid_interaction["data_mapping"]["from"] = "nonexistent-component"

    input_values = [["Quality A", "Quality C"]]
    output_results = process_interaction_action(
        plot_configs,
        input_values,
        default_outputs,
        invalid_interaction,
        triggers=default_triggers,
    )

    assert output_results == [no_update], "Expected no_update due to missing component"


def test_source_data_in_dataframe(
    sample_plot_configs,
    default_outputs,
    default_interaction,
    default_triggers,
    sample_source_data,
):
    """Test retrieving source data from 'dataframe' key."""
    plot_configs = copy.deepcopy(sample_plot_configs)
    # Remove 'kwargs' and add 'dataframe' directly to the component
    plot_configs["test-key"]["components"][0].pop("kwargs", None)
    plot_configs["test-key"]["components"][0]["dataframe"] = sample_source_data

    input_values = [["Quality A", "Quality C"]]
    output_results = process_interaction_action(
        plot_configs,
        input_values,
        default_outputs,
        default_interaction,
        triggers=default_triggers,
    )

    assert isinstance(output_results[0], Figure), "Expected a Plotly Figure output"


def test_missing_source_data(
    sample_plot_configs, default_outputs, default_interaction, default_triggers
):
    """Test handling when source data is missing."""
    plot_configs = copy.deepcopy(sample_plot_configs)
    # Remove both 'kwargs' and 'dataframe' from the source component
    plot_configs["test-key"]["components"][0].pop("kwargs", None)
    plot_configs["test-key"]["components"][0].pop("dataframe", None)

    input_values = [["Quality A", "Quality C"]]
    output_results = process_interaction_action(
        plot_configs,
        input_values,
        default_outputs,
        default_interaction,
        triggers=default_triggers,
    )

    assert output_results == [no_update], "Expected no_update due to missing data"


def test_apply_transformation(
    sample_plot_configs, default_outputs, default_interaction, default_triggers
):
    """Test applying a transformation to the data."""
    plot_configs = copy.deepcopy(sample_plot_configs)
    interaction_with_transformation = copy.deepcopy(default_interaction)
    interaction_with_transformation["data_processing"]["transformations"] = [
        {
            "type": "rename_columns",
            "params": {"columns": {"Measurement": "QualityMeasurement"}},
        }
    ]

    # Since the plot configuration still uses 'Measurement', we adjust our expectation
    input_values = [["Quality A", "Quality C"]]
    output_results = process_interaction_action(
        plot_configs,
        input_values,
        default_outputs,
        interaction_with_transformation,
        triggers=default_triggers,
    )

    fig = output_results[0]
    x_axis_title = fig.layout.xaxis.title.text
    # Expecting 'Measurement' instead of 'QualityMeasurement'
    assert (
        x_axis_title == "Measurement"
    ), f"Expected 'Measurement', got '{x_axis_title}'"


def test_update_config_with_dataframe(
    sample_plot_configs,
    default_outputs,
    default_interaction,
    default_triggers,
    sample_source_data,
):
    """Test updating component configuration using 'dataframe' key."""
    plot_configs = copy.deepcopy(sample_plot_configs)
    target_component = plot_configs["test-key"]["components"][0]
    # Remove 'kwargs' and use 'dataframe' instead
    target_component.pop("kwargs", None)
    target_component["dataframe"] = sample_source_data

    input_values = [["Quality A", "Quality C"]]
    output_results = process_interaction_action(
        plot_configs,
        input_values,
        default_outputs,
        default_interaction,
        triggers=default_triggers,
    )

    assert isinstance(output_results[0], Figure), "Expected a Plotly Figure output"


def test_target_component_table(
    sample_plot_configs, default_interaction, default_triggers, sample_source_data
):
    """Test handling a table target component."""
    plot_configs = copy.deepcopy(sample_plot_configs)
    # Add the table component
    plot_configs["test-key"]["components"].append(
        {
            "id": "data-table",
            "type": "table",
            "dataframe": sample_source_data,
            "columns": ["Measurement", "Value"],
        }
    )
    default_outputs = [{"component_id": "data-table", "component_property": "data"}]
    interaction_with_table = copy.deepcopy(default_interaction)
    interaction_with_table["data_mapping"]["to"] = "data-table"

    input_values = [["Quality A", "Quality C"]]
    output_results = process_interaction_action(
        plot_configs,
        input_values,
        default_outputs,
        interaction_with_table,
        triggers=default_triggers,
    )

    assert isinstance(
        output_results[0], list
    ), "Expected a list of dictionaries for table data"
    assert all(
        isinstance(row, dict) for row in output_results[0]
    ), "Each table row should be a dict"


def test_unknown_target_component_type(
    sample_plot_configs, default_outputs, default_interaction, default_triggers
):
    """Test handling an unknown target component type."""
    plot_configs = copy.deepcopy(sample_plot_configs)
    # Set an unknown type for the target component
    plot_configs["test-key"]["components"][0]["type"] = "unknown"

    input_values = [["Quality A", "Quality C"]]
    output_results = process_interaction_action(
        plot_configs,
        input_values,
        default_outputs,
        default_interaction,
        triggers=default_triggers,
    )

    assert output_results == [
        no_update
    ], "Expected no_update due to unknown component type"

import pytest
import pandas as pd
from plotly.graph_objects import Figure
from callbacks.plot_callbacks import process_interaction_action
from sampledata.plot_configs import plot_configs


def test_process_interaction_action():
    """
    Test the `process_interaction_action` function to ensure it processes interactions
    and produces the expected Plotly figure output.

    This function sets up source data, input values, outputs, interaction mappings,
    and triggers, then verifies that `process_interaction_action` correctly filters
    the data and returns a figure with the expected categories.
    """
    # Source data
    source_data = pd.DataFrame(
        {
            "Measurement": ["Quality A", "Quality B", "Quality A", "Quality C"],
            "Value": [60, 70, 65, 80],
        }
    )

    # Input values
    input_values = [["Quality A", "Quality C"]]

    # Expected outputs
    outputs = [
        {"component_id": "consumption-quality-box-plot", "component_property": "figure"}
    ]

    # Interaction definition
    interaction = {
        "action": "process_interaction",
        "data_mapping": {
            "from": "consumption-quality-box-plot",
            "to": "consumption-quality-box-plot",
        },
        "data_processing": {"filter": {"Measurement": {"in": "selected_measurements"}}},
    }

    # Trigger configuration
    triggers = [{"input_key": "selected_measurements"}]

    # Mock plot_configs for testing
    plot_configs["test-key"] = {
        "components": [
            {
                "id": "consumption-quality-box-plot",
                "type": "plot",
                "library": "px",
                "function": "box",
                "kwargs": {
                    "data_frame": source_data,
                    "x": "Measurement",
                    "y": "Value",
                },
                "layout_kwargs": {},
                "css": {},
            }
        ]
    }

    # Call the function
    output_results = process_interaction_action(
        plot_configs,
        input_values,
        outputs,
        interaction,
        triggers=triggers,
    )

    # Assertions to validate output
    assert len(output_results) == 1, "Expected one figure output"
    assert isinstance(
        output_results[0], Figure
    ), "Output should be a Plotly Figure instance"

    # Expected filtered data
    expected_df = source_data[
        source_data["Measurement"].isin(["Quality A", "Quality C"])
    ]

    # Collect category names from all traces in the figure
    fig = output_results[0]
    categories_in_plot = {trace.name for trace in fig.data}
    expected_categories = set(expected_df["Measurement"].unique())

    # Verify if the figure contains both expected categories
    assert (
        categories_in_plot == expected_categories
    ), "Figure categories should match filtered data categories"

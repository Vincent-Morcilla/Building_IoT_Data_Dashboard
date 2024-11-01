import pytest
import pandas as pd
from plotly.graph_objects import Figure
from callbacks.plot_callbacks import update_plot_property_action

from sampledata.plot_configs import sample_plot_configs as plot_configs


def test_update_plot_property_action():
    """Test the update_plot_property_action function with mock input data."""

    # Sample DataFrame
    data_frame = pd.DataFrame(
        {
            "Category": ["A", "B", "A", "B"],
            "Value": [1, 2, 3, 4],
            "Date": pd.date_range("2021-01-01", periods=4),
        }
    )

    # Update kwargs mapping
    update_kwargs = {"data_frame": "data_frame"}

    # Input values corresponding to triggers
    input_values = ["A", pd.Timestamp("2021-01-01"), pd.Timestamp("2021-01-03")]

    # Outputs
    outputs = [{"component_id": "test-plot", "component_property": "figure"}]

    # Filters to apply
    filters = {
        "Category": {"equals": "selected_category"},
        "Date": {"between": {"start_date": "start_date", "end_date": "end_date"}},
    }

    # Triggers
    triggers = [
        {"input_key": "selected_category"},
        {"input_key": "start_date"},
        {"input_key": "end_date"},
    ]

    # Data processing (none in this case)
    data_processing = {}

    # Mock plot_configs and find_component_by_id
    plot_configs["test-key"] = {
        "components": [
            {
                "id": "test-plot",
                "type": "plot",
                "library": "px",
                "function": "line",
                "kwargs": {
                    "x": "Date",
                    "y": "Value",
                },
                "layout_kwargs": {},
                "css": {},
            }
        ]
    }

    # Call the function
    output_results = update_plot_property_action(
        plot_configs,
        data_frame,
        update_kwargs,
        input_values,
        outputs,
        filters=filters,
        triggers=triggers,
        data_processing=data_processing,
    )

    # Assertions
    assert len(output_results) == 1
    assert isinstance(output_results[0], Figure)

    # Expected filtered DataFrame
    expected_df = data_frame[
        (data_frame["Category"] == "A")
        & (data_frame["Date"] >= pd.Timestamp("2021-01-01"))
        & (data_frame["Date"] <= pd.Timestamp("2021-01-03"))
    ]

    # Check if the figure data matches expected data
    fig_data = output_results[0].data[0]
    assert fig_data.x.tolist() == expected_df["Date"].tolist()
    assert fig_data.y.tolist() == expected_df["Value"].tolist()

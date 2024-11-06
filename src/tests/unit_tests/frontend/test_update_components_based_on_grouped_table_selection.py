import pytest
from unittest.mock import MagicMock, patch
from dash import html, no_update
import pandas as pd

from actions.update_components_based_on_grouped_table_selection import (
    update_components_based_on_grouped_table_selection_action,
    create_plot_component,
)


@pytest.fixture
def setup_data():
    """
    Fixture to set up sample data for the tests.

    Returns:
        dict: A dictionary containing setup data for the tests.
    """
    # Modified Stream IDs with unique first six characters
    table_data = pd.DataFrame(
        {
            "Stream ID": ["strA01", "strB02"],
            "Brick Class": ["Class A", "Class A"],
            "Is Step Function": [True, False],
        }
    )

    # Sample grouped_table_data
    grouped_table_data = pd.DataFrame(
        {
            "Brick Class": ["Class A", "Class B"],
            "Step Function Percentage": [50, 100],
        }
    )

    # Mock database manager
    db = MagicMock()

    # Configure db.get_stream to return sample dataframes for the new Stream IDs
    stream_data_A1 = pd.DataFrame(
        {
            "time": pd.date_range(start="2021-01-01", periods=10, freq="h"),
            "value": range(10),
        }
    )
    stream_data_B2 = pd.DataFrame(
        {
            "time": pd.date_range(start="2021-01-01", periods=10, freq="h"),
            "value": range(10, 20),
        }
    )

    def get_stream_mock(stream_id):
        if stream_id == "strA01":
            return stream_data_A1
        elif stream_id == "strB02":
            return stream_data_B2
        else:
            return pd.DataFrame()

    db.get_stream.side_effect = get_stream_mock

    # Sample interaction configuration
    interaction = {
        "data_source": {
            "table_data": table_data,
            "grouped_table_data": grouped_table_data,
            "db": db,
        },
        "index_column": "Brick Class",
    }

    # Sample triggers
    triggers = [
        {
            "component_id": "grouped_datatable",
            "component_property": "selected_rows",
            "input_key": "selected_rows",
        }
    ]

    # Sample outputs
    outputs = [{"component_id": "output_div", "component_property": "children"}]

    plot_configs = {}

    return {
        "table_data": table_data,
        "grouped_table_data": grouped_table_data,
        "db": db,
        "interaction": interaction,
        "triggers": triggers,
        "outputs": outputs,
        "plot_configs": plot_configs,
    }


def test_valid_input(setup_data):
    """
    Test the function with valid inputs.
    """
    input_values = [[0]]  # Simulate selecting the first row

    with patch(
        "actions.update_components_based_on_grouped_table_selection.create_plot_component"
    ) as mock_create_plot_component:
        # Mock the plot component creation
        mock_create_plot_component.return_value = html.Div("Plot Component")

        # Call the function
        result = update_components_based_on_grouped_table_selection_action(
            setup_data["plot_configs"],
            input_values,
            setup_data["outputs"],
            setup_data["interaction"],
            setup_data["triggers"],
        )

        # Assertions
        assert isinstance(result, list)
        assert len(result) == 1
        assert isinstance(result[0], list)
        assert len(result[0]) == 1
        component = result[0][0]
        assert isinstance(component, html.Div)
        assert component.children == "Plot Component"


def test_no_streams_data(setup_data):
    """
    Test the function when no streams data is available.
    """
    # Remove side_effect to ensure return_value is used
    setup_data["db"].get_stream.side_effect = None
    # Modify db.get_stream to return empty dataframes
    setup_data["db"].get_stream.return_value = pd.DataFrame()

    input_values = [[0]]  # Selected row

    result = update_components_based_on_grouped_table_selection_action(
        setup_data["plot_configs"],
        input_values,
        setup_data["outputs"],
        setup_data["interaction"],
        setup_data["triggers"],
    )

    # Assertions
    assert isinstance(result, list)
    assert len(result) == 1
    assert isinstance(result[0], list)
    component = result[0][0]
    assert isinstance(component, html.Div)

    # Check that the "No Data Available" message is returned
    assert "No Data Available" in component.children[0].children


def test_exception_during_processing(setup_data):
    """
    Test the function when an exception occurs during data processing.
    """

    # Simulate an exception in db.get_stream
    def get_stream_exception(_):
        raise Exception("Test exception")

    setup_data["db"].get_stream.side_effect = get_stream_exception

    input_values = [[0]]  # Selected row

    result = update_components_based_on_grouped_table_selection_action(
        setup_data["plot_configs"],
        input_values,
        setup_data["outputs"],
        setup_data["interaction"],
        setup_data["triggers"],
    )

    # Should return an error message
    assert isinstance(result, list)
    assert len(result) == 1
    assert isinstance(result[0], list)
    component = result[0][0]
    assert isinstance(component, html.Div)
    assert "Error Processing Data" in component.children[0].children

import pytest
from unittest.mock import MagicMock, patch
import warnings
from dash import html, no_update
import pandas as pd
from actions.update_components_based_on_grouped_table_selection import (
    update_components_based_on_grouped_table_selection_action,
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


def test_stream_df_empty(setup_data):
    """
    Test the function when stream_df is empty for a stream.
    """

    # Modify db.get_stream to return an empty DataFrame with correct columns
    def get_stream_empty(stream_id):
        return pd.DataFrame(columns=["time", "value"])

    setup_data["db"].get_stream.side_effect = get_stream_empty

    input_values = [[0]]  # Selected row

    result = update_components_based_on_grouped_table_selection_action(
        setup_data["plot_configs"],
        input_values,
        setup_data["outputs"],
        setup_data["interaction"],
        setup_data["triggers"],
    )

    # Check that the "No Data Available" message is returned
    assert isinstance(result, list)
    assert len(result) == 1
    component = result[0][0]
    assert isinstance(component, html.Div)
    assert "No Data Available" in component.children[0].children


def test_stream_processing_exception(setup_data):
    """
    Test the function when an exception occurs during stream processing.
    """

    # Simulate an exception in the stream processing by providing invalid date formats
    def get_stream_invalid_data(stream_id):
        return pd.DataFrame({"time": ["invalid_date"] * 10, "value": range(10)})

    setup_data["db"].get_stream.side_effect = get_stream_invalid_data

    input_values = [[0]]  # Selected row

    with warnings.catch_warnings():
        # Suppress the UserWarning triggered by invalid date parsing during the test
        warnings.simplefilter("ignore", UserWarning)
        result = update_components_based_on_grouped_table_selection_action(
            setup_data["plot_configs"],
            input_values,
            setup_data["outputs"],
            setup_data["interaction"],
            setup_data["triggers"],
        )

    # Should handle the exception and proceed without crashing
    assert isinstance(result, list)
    assert len(result) == 1
    component = result[0][0]
    assert isinstance(component, html.Div)
    assert "No Data Available" in component.children[0].children


def test_should_highlight_else_branch(setup_data):
    """
    Test the function where step_function_pct > 50 to execute the else branch.
    """
    # Modify the Step Function Percentage to be greater than 50 and not equal to 100
    setup_data["grouped_table_data"]["Step Function Percentage"] = [60, 100]
    setup_data["interaction"]["data_source"]["grouped_table_data"] = setup_data[
        "grouped_table_data"
    ]

    input_values = [[0]]  # Selected row

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

        # Ensure the mock was called
        mock_create_plot_component.assert_called()
        args, kwargs = mock_create_plot_component.call_args

        # Extract plot_component from args
        plot_component = args[0]

        # Access the color sequence from the plot_component
        color_sequence = plot_component["kwargs"]["color_discrete_sequence"]

        # Since 'Is Step Function' is [True, False], 'not is_step' should invert it
        expected_colors = [
            "#808080" if not is_step else None
            for is_step in setup_data["table_data"]["Is Step Function"]
        ]

        assert color_sequence == expected_colors


def test_invalid_configuration_no_db(setup_data):
    """
    Test the function when the database connection is not provided.
    """
    setup_data["interaction"]["data_source"]["db"] = None

    input_values = [[0]]  # Selected row

    result = update_components_based_on_grouped_table_selection_action(
        setup_data["plot_configs"],
        input_values,
        setup_data["outputs"],
        setup_data["interaction"],
        setup_data["triggers"],
    )

    # Should return a configuration error message
    assert isinstance(result, list)
    assert len(result) == 1
    component = result[0][0]
    assert isinstance(component, html.Div)
    assert "Configuration Error" in component.children[0].children
    assert "Database connection not provided" in component.children[1].children


def test_invalid_configuration_wrong_index_column(setup_data):
    """
    Test the function when index_column is not 'Brick Class'.
    """
    setup_data["interaction"]["index_column"] = "Wrong Column"

    input_values = [[0]]  # Selected row

    result = update_components_based_on_grouped_table_selection_action(
        setup_data["plot_configs"],
        input_values,
        setup_data["outputs"],
        setup_data["interaction"],
        setup_data["triggers"],
    )

    # Should return a configuration error message
    assert isinstance(result, list)
    assert len(result) == 1
    component = result[0][0]
    assert isinstance(component, html.Div)
    assert "Configuration Error" in component.children[0].children
    assert (
        "Expected index_column 'Brick Class' but got 'Wrong Column'"
        in component.children[1].children
    )


def test_no_selected_rows(setup_data):
    """
    Test the function when no rows are selected.
    """
    input_values = [[]]  # No rows selected

    result = update_components_based_on_grouped_table_selection_action(
        setup_data["plot_configs"],
        input_values,
        setup_data["outputs"],
        setup_data["interaction"],
        setup_data["triggers"],
    )

    # Should return [no_update] * len(outputs)
    assert result == [no_update] * len(setup_data["outputs"])


def test_no_input_values(setup_data):
    """
    Test the function when input_values is empty or None.
    """
    # Test with empty input_values
    input_values = []
    result = update_components_based_on_grouped_table_selection_action(
        setup_data["plot_configs"],
        input_values,
        setup_data["outputs"],
        setup_data["interaction"],
        setup_data["triggers"],
    )
    assert result == [no_update] * len(setup_data["outputs"])

    # Test with input_values as None
    input_values = None
    result = update_components_based_on_grouped_table_selection_action(
        setup_data["plot_configs"],
        input_values,
        setup_data["outputs"],
        setup_data["interaction"],
        setup_data["triggers"],
    )
    assert result == [no_update] * len(setup_data["outputs"])

import pytest
from unittest.mock import patch
from dash import html
import pandas as pd

from actions.update_components_based_on_table_selection import (
    update_components_based_on_table_selection_action,
)


@pytest.fixture
def setup_data():
    """
    Fixture to set up sample data for the tests.

    Returns:
        dict: A dictionary containing setup data for the tests.
    """
    # Sample table data
    table_data = pd.DataFrame({"StreamIDs": ["stream1", "stream2"], "Data": [10, 20]})

    # Sample data_dict mapping StreamIDs to component lists
    data_dict = {
        "stream1": [{"type": "plot", "id": "plot1"}],
        "stream2": [{"type": "plot", "id": "plot2"}],
    }

    # Sample interaction configuration
    interaction = {
        "data_source": {"table_data": table_data, "data_dict": data_dict},
        "index_column": "StreamIDs",
    }

    # Sample triggers
    triggers = [
        {
            "component_id": "datatable",
            "component_property": "selected_rows",
            "input_key": "selected_rows",
        }
    ]

    # Sample outputs
    outputs = [{"component_id": "output_div", "component_property": "children"}]

    plot_configs = {}

    return {
        "table_data": table_data,
        "data_dict": data_dict,
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
        "actions.update_components_based_on_table_selection.create_plot_component"
    ) as mock_create_plot_component, patch(
        "actions.update_components_based_on_table_selection.create_ui_component"
    ) as mock_create_ui_component, patch(
        "actions.update_components_based_on_table_selection.create_table_component"
    ) as mock_create_table_component:
        # Mock the component creation functions
        mock_create_plot_component.return_value = html.Div("Plot Component")
        mock_create_ui_component.return_value = html.Div("UI Component")
        mock_create_table_component.return_value = html.Div("Table Component")

        # Call the function
        result = update_components_based_on_table_selection_action(
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


def test_no_row_selected(setup_data):
    """
    Test the function when no row is selected.
    """
    input_values = [[]]  # Empty selected_rows

    with pytest.raises(ValueError) as exc_info:
        update_components_based_on_table_selection_action(
            setup_data["plot_configs"],
            input_values,
            setup_data["outputs"],
            setup_data["interaction"],
            setup_data["triggers"],
        )

    assert "No row selected in the DataTable. Please select a row." in str(
        exc_info.value
    )


def test_missing_data_source(setup_data):
    """
    Test when data_source is missing in interaction.
    """
    setup_data["interaction"]["data_source"] = None  # Remove data_source

    input_values = [[0]]  # selected_rows

    with pytest.raises(ValueError) as exc_info:
        update_components_based_on_table_selection_action(
            setup_data["plot_configs"],
            input_values,
            setup_data["outputs"],
            setup_data["interaction"],
            setup_data["triggers"],
        )

    assert "Data source is missing or invalid in interaction configuration." in str(
        exc_info.value
    )


def test_invalid_table_data(setup_data):
    """
    Test when table_data cannot be converted to DataFrame.
    """
    setup_data["interaction"]["data_source"][
        "table_data"
    ] = "invalid_data"  # Non-convertible data

    input_values = [[0]]  # selected_rows

    with pytest.raises(ValueError) as exc_info:
        update_components_based_on_table_selection_action(
            setup_data["plot_configs"],
            input_values,
            setup_data["outputs"],
            setup_data["interaction"],
            setup_data["triggers"],
        )

    assert "Failed to convert table_data to DataFrame" in str(exc_info.value)


def test_index_column_not_in_table_data(setup_data):
    """
    Test when index_column is not in table_data columns.
    """
    setup_data["interaction"][
        "index_column"
    ] = "NonExistentColumn"  # Invalid index column

    input_values = [[0]]  # selected_rows

    with pytest.raises(ValueError) as exc_info:
        update_components_based_on_table_selection_action(
            setup_data["plot_configs"],
            input_values,
            setup_data["outputs"],
            setup_data["interaction"],
            setup_data["triggers"],
        )

    assert "Index column 'NonExistentColumn' not found in table_data." in str(
        exc_info.value
    )


def test_selected_row_out_of_bounds(setup_data):
    """
    Test when selected row index is out of bounds.
    """
    input_values = [[10]]  # selected_rows with index out of bounds

    with pytest.raises(IndexError) as exc_info:
        update_components_based_on_table_selection_action(
            setup_data["plot_configs"],
            input_values,
            setup_data["outputs"],
            setup_data["interaction"],
            setup_data["triggers"],
        )

    assert "Selected row index 10 is out of bounds." in str(exc_info.value)


def test_selected_index_value_not_in_data_dict(setup_data):
    """
    Test when selected index value is not in data_dict.
    """
    # Modify table_data to have an index value not in data_dict
    setup_data["table_data"].loc[0, "StreamIDs"] = "nonexistent_stream"
    setup_data["interaction"]["data_source"]["table_data"] = setup_data["table_data"]

    input_values = [[0]]  # selected_rows

    with pytest.raises(KeyError) as exc_info:
        update_components_based_on_table_selection_action(
            setup_data["plot_configs"],
            input_values,
            setup_data["outputs"],
            setup_data["interaction"],
            setup_data["triggers"],
        )

    assert "No components found for index value 'nonexistent_stream'." in str(
        exc_info.value
    )


def test_selected_components_not_a_list(setup_data):
    """
    Test when selected_components is not a list.
    """
    setup_data["data_dict"]["stream1"] = "not_a_list"  # Invalid components entry

    input_values = [[0]]  # selected_rows

    with pytest.raises(TypeError) as exc_info:
        update_components_based_on_table_selection_action(
            setup_data["plot_configs"],
            input_values,
            setup_data["outputs"],
            setup_data["interaction"],
            setup_data["triggers"],
        )

    assert "The components for index value 'stream1' are not in a list." in str(
        exc_info.value
    )


def test_component_creation_failure(setup_data):
    """
    Test when component creation fails.
    """
    input_values = [[0]]  # selected_rows

    with patch(
        "actions.update_components_based_on_table_selection.create_plot_component"
    ) as mock_create_plot_component:
        # Simulate an exception during component creation
        mock_create_plot_component.side_effect = Exception("Mocked exception")

        with pytest.raises(Exception) as exc_info:
            update_components_based_on_table_selection_action(
                setup_data["plot_configs"],
                input_values,
                setup_data["outputs"],
                setup_data["interaction"],
                setup_data["triggers"],
            )

        assert "Mocked exception" in str(exc_info.value)

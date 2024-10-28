"""Test for update_plot_based_on_table_selection callback function."""

import pytest
import pandas as pd
from plotly.graph_objects import Figure
from callbacks.plot_callbacks import update_plot_based_on_table_selection


def test_update_plot_based_on_table_selection():
    """
    Test the update_plot_based_on_table_selection function to ensure it correctly updates the plot
    based on selected rows in a data table.

    The test checks if:
        - The function returns the expected number of outputs.
        - The output is an instance of a Plotly Figure.
        - The figure contains the expected data traces corresponding to the timeseries.
    """
    # Sample table data
    table_data = pd.DataFrame({
        'ID': [1, 2, 3],
        'Name': ['Room A', 'Room B', 'Room C']
    })

    # Timeseries data for each ID
    timeseries_data_dict = {
        1: pd.DataFrame({
            'Date': pd.date_range('2021-01-01', periods=3),
            'Value': [10, 20, 30],
            'Variable1': [5, 15, 25],
            'Variable2': [2, 12, 22],
        }),
        2: pd.DataFrame({
            'Date': pd.date_range('2021-01-01', periods=3),
            'Value': [15, 25, 35],
            'Variable1': [7, 17, 27],
            'Variable2': [3, 13, 23],
        }),
        3: pd.DataFrame({
            'Date': pd.date_range('2021-01-01', periods=3),
            'Value': [20, 30, 40],
            'Variable1': [10, 20, 30],
            'Variable2': [4, 14, 24],
        }),
    }

    # Input values (selected_rows)
    input_values = [[1]]  # Selecting the second row (index 1)

    # Outputs
    outputs = [{
        'component_id': 'updateable-line-chart',
        'component_property': 'figure'
    }]

    # Interaction
    interaction = {
        'action': 'update_plot_based_on_table_selection',
        'data_source': {
            'table_data': table_data,
            'timeseries_data_dict': timeseries_data_dict,
        }
    }

    # Triggers
    triggers = [{'input_key': 'selected_rows'}]

    # Call the function
    output_results = update_plot_based_on_table_selection(
        input_values,
        outputs,
        interaction,
        triggers=triggers,
    )

    # Assertions
    assert len(output_results) == 1, "The function should return one output."
    assert isinstance(output_results[0], Figure), "Output should be a Plotly Figure instance."

    # Expected timeseries data for ID 2
    expected_df = timeseries_data_dict[2]

    # Check if the figure has the correct number of traces
    assert len(output_results[0].data) == 3, "Figure should have three data traces."

    # Check each trace
    for trace, column in zip(output_results[0].data, ['Value', 'Variable1', 'Variable2']):
        assert trace.x.tolist() == expected_df['Date'].tolist(), f"X-axis for {column} is incorrect."
        assert trace.y.tolist() == expected_df[column].tolist(), f"Y-axis for {column} is incorrect."

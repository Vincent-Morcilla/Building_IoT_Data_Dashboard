"""
Updates components based on the selected row in a table.

This module handles the logic for updating components when a user selects a row
in a DataTable. It retrieves the selected data, finds associated components, and
generates dynamic content such as plots and tables based on the selection,
enhancing the interactivity and user engagement within the app.
"""

from typing import List, Dict, Any

from dash import html
import pandas as pd

from components.plot_generator import (
    create_plot_component,
    create_ui_component,
    create_table_component,
)
from models.types import PlotConfig


def update_components_based_on_table_selection_action(
    plot_configs: PlotConfig,
    input_values: List[Any],
    outputs: List[Dict[str, Any]],
    interaction: Dict[str, Any],
    triggers: List[Dict[str, Any]] = None,
) -> List[Any]:
    """
    Update components based on the selected row in the table.

    Args:
        plot_configs (PlotConfig): The plot configurations.
        input_values (List[Any]): The input values from the triggers.
        outputs (List[Dict[str, Any]]): The outputs to update.
        interaction (Dict[str, Any]): Interaction configuration.
        triggers (List[Dict[str, Any]], optional): Trigger configurations.

    Returns:
        List[Any]: Updated outputs for the Dash components.
    """
    output_results = []

    # Map input values to their corresponding trigger keys
    input_mapping = {
        trigger.get("input_key", f"input_{i}"): value
        for i, (trigger, value) in enumerate(zip(triggers or [], input_values))
    }

    # Get selected row index
    selected_rows = input_mapping.get("selected_rows", [])
    if not selected_rows:
        error_message = "No row selected in the DataTable. Please select a row."
        print(f"Error: {error_message}")
        raise ValueError(error_message)

    row_idx = selected_rows[0]

    # Retrieve data sources from interaction dictionary
    data_source = interaction.get("data_source")
    if data_source is None or not isinstance(data_source, dict):
        error_message = (
            "Data source is missing or invalid in interaction configuration."
        )
        print(f"Error: {error_message}")
        raise ValueError(error_message)

    table_data = data_source.get("table_data")
    data_dict = data_source.get("data_dict")
    index_column = interaction.get("index_column")

    # Verify data availability
    if table_data is None or data_dict is None or index_column is None:
        error_message = (
            "Data source or index column not found in interaction configuration."
        )
        print(f"Error: {error_message}")
        raise ValueError(error_message)

    # Convert table_data to DataFrame if it's not already
    if not isinstance(table_data, pd.DataFrame):
        try:
            table_data = pd.DataFrame(table_data)
        except Exception as e:
            error_message = f"Failed to convert table_data to DataFrame: {e}"
            print(f"Error: {error_message}")
            raise ValueError(error_message)

    # Check if index_column exists in table_data
    if index_column not in table_data.columns:
        error_message = f"Index column '{index_column}' not found in table_data."
        print(f"Error: {error_message}")
        raise ValueError(error_message)

    # Check if the selected row index is within the range of the table data
    if row_idx < 0 or row_idx >= len(table_data):
        error_message = f"Selected row index {row_idx} is out of bounds."
        print(f"Error: {error_message}")
        raise IndexError(error_message)

    # Retrieve selected index value
    selected_index_value = table_data.iloc[row_idx][index_column]

    # Check if the selected index value exists in data_dict
    if selected_index_value not in data_dict:
        error_message = f"No components found for index value '{selected_index_value}'. Ensure that this value exists in the data_dict."
        print(f"Error: {error_message}")
        raise KeyError(error_message)

    # Get components associated with the selected index
    selected_components = data_dict.get(selected_index_value)

    # Verify that selected_components is a list
    if not isinstance(selected_components, list):
        error_message = f"The components for index value '{selected_index_value}' are not in a list."
        print(f"Error: {error_message}")
        raise TypeError(error_message)

    # Generate the components
    dynamic_components = []
    for component in selected_components:
        comp_type = component.get("type")
        if comp_type == "plot":
            dynamic_components.append(create_plot_component(component))
        elif comp_type == "table":
            dynamic_components.append(create_table_component(component))
        elif comp_type == "UI":
            dynamic_components.append(create_ui_component(component))
        elif comp_type == "separator":
            separator_style = component.get("style", {})
            dynamic_components.append(html.Hr(style=separator_style))
        else:
            error_message = f"Unsupported component type '{comp_type}' in data_dict."
            print(f"Error: {error_message}")
            raise ValueError(error_message)

    output_results.append(dynamic_components)

    return output_results

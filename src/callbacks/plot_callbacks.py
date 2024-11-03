import copy
from typing import List, Dict, Any
from dash import html, no_update
from dash.dependencies import Input, Output
import pandas as pd
from components.plot_generator import (
    create_plot_component,
    find_component_by_id,
    create_ui_component,
    create_table_component,
)
from helpers.data_processing import apply_generic_filters, apply_transformation


def process_interaction_action(
    plot_configs,  # FIXME: Add type hint
    input_values: List[Any],
    outputs: List[Dict[str, Any]],
    interaction: Dict[str, Any],
    triggers: List[Dict[str, Any]],
) -> List[Any]:
    """Process interactions between components based on configuration.

    Args:
        input_values: The input values from the triggers.
        outputs: The output configurations.
        interaction: The interaction configuration from plot_configs.
        triggers: List of triggers from the interaction.

    Returns:
        The updated outputs for the Dash components.
    """
    output_results = []

    # Map input_keys to input_values
    input_mapping = {
        trigger.get("input_key", f"input_{i}"): value
        for i, (trigger, value) in enumerate(zip(triggers, input_values))
    }

    data_processing = interaction.get("data_processing", {})
    data_mapping = interaction.get("data_mapping", {})

    # Get source and target component IDs
    source_id = data_mapping.get("from")
    target_id = data_mapping.get("to")

    # Retrieve source and target components
    try:
        source_component = find_component_by_id(source_id, plot_configs)
        target_component = find_component_by_id(target_id, plot_configs)
    except ValueError as error:
        print(error)
        return [no_update] * len(outputs)

    # Get data from source component
    source_data = source_component.get("kwargs", {}).get("data_frame")
    if source_data is None:
        source_data = source_component.get("dataframe")
    if source_data is None:
        print(f"Source component '{source_id}' does not contain data.")
        return [no_update] * len(outputs)

    # Process data according to data_processing instructions
    processed_data = source_data.copy()

    # Apply filters if any
    filters = data_processing.get("filter")
    if filters:
        processed_data = apply_generic_filters(processed_data, filters, input_mapping)

    # Apply transformations if any
    transformations = data_processing.get("transformations", [])
    for transformation in transformations:
        processed_data = apply_transformation(
            processed_data, transformation, input_mapping
        )

    # Prepare the updated component configuration
    updated_component_config = copy.deepcopy(target_component)
    if "kwargs" in updated_component_config:
        updated_component_config["kwargs"]["data_frame"] = processed_data
    elif "dataframe" in updated_component_config:
        updated_component_config["dataframe"] = processed_data

    # Rebuild the component
    if target_component["type"] == "plot":
        updated_component = create_plot_component(updated_component_config)
        output_results.append(updated_component.figure)
    elif target_component["type"] == "table":
        data = processed_data.to_dict("records")
        output_results.append(data)
    else:
        output_results.append(no_update)

    return output_results


def update_components_based_on_table_selection_action(
    plot_configs,
    input_values,
    outputs,
    interaction,
    triggers=None,
):
    """
    Update components based on the selected row in the table.

    Args:
        plot_configs: The plot configurations.
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


def register_plot_callbacks(app, plot_configs):
    """Register callbacks for the plot configurations.

    Args:
        app (dash.Dash): The Dash app instance.
        plot_configs (dict): The plot configurations.
    """
    action_functions = {
        "process_interaction": process_interaction_action,
        "update_components_based_on_table_selection": update_components_based_on_table_selection_action,
    }

    for category_key, config in plot_configs.items():
        interactions = config.get("interactions", [])
        for interaction in interactions:
            triggers = interaction.get("triggers", [])
            if not triggers:
                continue

            outputs = interaction["outputs"]
            action = interaction.get("action")
            inputs = [
                Input(trigger["component_id"], trigger["component_property"])
                for trigger in triggers
            ]
            output_objs = [
                Output(output["component_id"], output["component_property"])
                for output in outputs
            ]

            def generate_callback(
                action=action,
                interaction=interaction,
                triggers=triggers,
                outputs=outputs,
            ):
                """Generate the callback function for the interaction."""

                def callback(*input_values):
                    """The callback function to be registered with Dash."""
                    action_func = action_functions.get(action)
                    if action_func:
                        if action == "update_plot_property":
                            # Extract data_frame and update_kwargs from interaction
                            data_source_id = interaction.get("data_source")
                            try:
                                data_source_component = find_component_by_id(
                                    data_source_id, plot_configs
                                )
                            except ValueError as error:
                                print(error)
                                return [no_update] * len(outputs)

                            # Retrieve data_frame based on whether component is `px` or `go`
                            library = data_source_component.get("library")
                            if library == "px":
                                data_frame = data_source_component.get(
                                    "kwargs", {}
                                ).get("data_frame")
                            elif library == "go":
                                data_frame = data_source_component.get("data_frame")
                            else:
                                data_frame = None

                            # Check if data_frame exists after retrieval
                            if data_frame is None:
                                print(
                                    f"Data source component '{data_source_id}' does not contain data."
                                )
                                return [no_update] * len(outputs)

                            update_kwargs = interaction.get("update_kwargs", {})
                            filters = interaction.get("filters", {})
                            data_processing = interaction.get("data_processing", {})

                            return action_func(
                                plot_configs,
                                data_frame,
                                update_kwargs,
                                input_values,
                                outputs,
                                filters=filters,
                                triggers=triggers,
                                data_processing=data_processing,
                            )
                        else:
                            # For other actions, call with input_values, outputs, interaction, triggers
                            return action_func(
                                plot_configs,
                                input_values,
                                outputs,
                                interaction,
                                triggers=triggers,
                            )
                    return [no_update] * len(outputs)

                return callback

            # Register the callback with the app
            callback_func = generate_callback(
                action=action,
                interaction=interaction,
                triggers=triggers,
                outputs=outputs,
            )
            app.callback(output_objs, inputs)(callback_func)

import copy
from typing import List, Dict, Any
from dash import no_update
from dash.dependencies import Input, Output
import pandas as pd
import plotly.express as px
from components.plot_generator import (
    create_plot_component,
    find_component_by_id,
)
from helpers.data_processing import apply_generic_filters, apply_transformation


def update_plot_property_action(
    plot_configs,  # FIXME: Add type hint
    data_frame: pd.DataFrame,
    update_kwargs: Dict[str, Any],
    input_values: List[Any],
    outputs: List[Dict[str, Any]],
    filters: Dict[str, Any] = None,
    triggers: List[Dict[str, Any]] = None,
    data_processing: Dict[str, Any] = None,
) -> List[Any]:
    """Update plot properties, including data filtering and processing.

    Args:
        data_frame: The original DataFrame.
        update_kwargs: Mapping of plot kwargs to input keys.
        input_values: The input values from the triggers.
        outputs: The output configurations.
        filters: Filters to apply to the data.
        triggers: List of triggers from the interaction.
        data_processing: Instructions for processing the data, such as resampling.

    Returns:
        The updated figures or components.
    """
    output_results = []

    # Map input_keys to input_values
    input_mapping = {
        trigger.get("input_key", f"input_{i}"): value
        for i, (trigger, value) in enumerate(zip(triggers, input_values))
    }

    # Apply filters
    if filters:
        data_frame = apply_generic_filters(data_frame, filters, input_mapping)

    # Apply data processing
    if data_processing:
        transformations = data_processing.get("transformations", [])
        for transformation in transformations:
            data_frame = apply_transformation(data_frame, transformation, input_mapping)

    for output in outputs:
        component_id = output["component_id"]
        component_property = output["component_property"]
        component_config = find_component_by_id(component_id, plot_configs)
        component_type = component_config["type"]

        if component_type == "plot":
            new_component_config = copy.deepcopy(component_config)
            plot_kwargs = new_component_config.get("kwargs", {})

            # Update kwargs with new data frame
            for kwarg_key, input_key in update_kwargs.items():
                if kwarg_key == "data_frame":
                    plot_kwargs[kwarg_key] = data_frame.copy()
                else:
                    plot_kwargs[kwarg_key] = input_mapping.get(input_key, input_key)

            # Rebuild the plot component
            updated_plot = create_plot_component(new_component_config)
            if component_property == "figure":
                output_results.append(updated_plot.figure)
            else:
                output_results.append(updated_plot)
        elif component_type == "table":
            data = data_frame.to_dict("records")
            output_results.append(data)
        else:
            output_results.append(no_update)

    return output_results


def update_table_based_on_plot_click_action(
    plot_configs,  # FIXME: Add type hint
    input_values: List[Any],
    outputs: List[Dict[str, Any]],
    interaction: Dict[str, Any],
    triggers: List[Dict[str, Any]],
) -> List[Any]:
    """Update the table based on a click event on a plot.

    Args:
        input_values: The input values from the triggers.
        outputs: The output configurations.
        interaction: The interaction configuration from plot_configs.
        triggers: List of triggers from the interaction.

    Returns:
        The updated table data.
    """
    return process_interaction_action(
        plot_configs,
        input_values=input_values,
        outputs=outputs,
        interaction=interaction,
        triggers=triggers,
    )


def update_plot_based_on_table_selection(
    plot_configs,  # FIXME: Add type hint
    input_values: List[Any],
    outputs: List[Dict[str, Any]],
    interaction: Dict[str, Any],
    triggers: List[Dict[str, Any]] = None,
) -> List[Any]:
    """
    Update the plot based on the selected row in the table.

    Parameters:
        input_values (List[Any]): Values provided as inputs to the function.
        outputs (List[Dict[str, Any]]): List of output configurations for the function.
        interaction (Dict[str, Any]): Contains interaction settings including data sources.
        triggers (List[Dict[str, Any]], optional): Trigger configurations for dynamic inputs.

    Returns:
        List[Any]: Updated list of output figures or no updates if data is missing.
    """
    output_results = []

    # Map input values to their corresponding trigger keys
    input_mapping = {
        trigger.get("input_key", f"input_{i}"): value
        for i, (trigger, value) in enumerate(zip(triggers or [], input_values))
    }

    # Get selected row index, defaulting to the first row if not specified
    selected_rows = input_mapping.get("selected_rows", [])
    row_idx = selected_rows[0] if selected_rows else 0

    # Retrieve data sources from interaction dictionary
    data_source = interaction.get("data_source", {})
    table_data = data_source.get("table_data")
    timeseries_data_dict = data_source.get("timeseries_data_dict")

    # Verify data availability
    if table_data is None or timeseries_data_dict is None:
        print("Data source not found in interaction configuration.")
        return [no_update] * len(outputs)

    # Retrieve selected row's ID and corresponding timeseries data
    selected_id = table_data.iloc[row_idx]["ID"]
    filtered_data = timeseries_data_dict.get(selected_id)

    if filtered_data is None:
        print(f"No timeseries data found for ID {selected_id}.")
        return [no_update] * len(outputs)

    # Generate the timeseries plot figure
    fig = px.line(
        data_frame=filtered_data,
        x="Date",
        y=["Value", "Variable1", "Variable2"],
        title=f"Timeseries for {table_data.iloc[row_idx]['Name']}",
    )
    fig.update_layout(
        font=dict(color="black"),
        title=dict(
            text=f"Timeseries for {table_data.iloc[row_idx]['Name']}",
            xanchor="center",
            x=0.5,
        ),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=-0.3,
            xanchor="center",
            x=0.5,
        ),
    )

    # Add the updated figure to the output results
    output_results.append(fig)

    return output_results


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


def register_plot_callbacks(app, plot_configs):
    """Register callbacks for the plot configurations.

    Args:
        app (dash.Dash): The Dash app instance.
        plot_configs (dict): The plot configurations.
    """
    action_functions = {
        "update_plot_property": update_plot_property_action,
        "update_table_based_on_plot_click": update_table_based_on_plot_click_action,
        "process_interaction": process_interaction_action,
        "update_plot_based_on_table_selection": update_plot_based_on_table_selection,
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

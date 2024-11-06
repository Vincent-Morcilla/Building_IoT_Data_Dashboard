import copy
from typing import List, Dict, Any
from dash import Dash, html, no_update
from dash.dependencies import Input, Output
import pandas as pd
from components.plot_generator import (
    create_plot_component,
    find_component_by_id,
    create_ui_component,
    create_table_component,
)
from helpers.data_processing import apply_generic_filters, apply_transformation
from models.types import PlotConfig


def process_interaction_action(
    plot_configs: PlotConfig,
    input_values: List[Any],
    outputs: List[Dict[str, Any]],
    interaction: Dict[str, Any],
    triggers: List[Dict[str, Any]],
) -> List[Any]:
    """Process interactions between components based on configuration.

    Args:
        plot_configs (PlotConfig): The plot configuration dictionary.
        input_values (List[Any]): The input values from the triggers.
        outputs (List[Dict[str, Any]]): The output configurations.
        interaction (Dict[str, Any]): The interaction configuration from plot_configs.
        triggers (List[Dict[str, Any]]): List of triggers from the interaction.

    Returns:
        List[Any]: The updated outputs for the Dash components.
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


def update_components_based_on_grouped_table_selection_action(
    plot_configs,
    input_values,
    outputs,
    interaction,
    triggers=None,
):
    output_results = []

    selected_rows = input_values[0] if input_values else []
    if not selected_rows:
        return [no_update] * len(outputs)

    data_source = interaction.get("data_source", {})
    table_data = data_source.get("table_data")
    grouped_table_data = data_source.get("grouped_table_data")
    # table_data = table_data.sort_values("Brick Class")#.reset_index(drop=True)
    db = data_source.get("db")
    index_column = interaction.get("index_column")

    # Handle DB-based visualization (Brick Class view)
    if db is not None and index_column == "Brick Class":
        try:
            grouped_table_data = grouped_table_data.sort_values(
                "Brick Class"
            ).reset_index(drop=True)
            selected_value = grouped_table_data.iloc[selected_rows[0]][index_column]
            streams_df = pd.DataFrame()
            first = True

            # Get all streams for this class
            class_streams = table_data[table_data["Brick Class"] == selected_value][
                "Stream ID"
            ]
            for stream_id in class_streams:
                stream_df = db.get_stream(stream_id)

                # Ensure we have the expected columns
                if "time" not in stream_df.columns or "value" not in stream_df.columns:
                    continue

                try:
                    # Convert value to numeric, dropping non-numeric values
                    stream_df["value"] = pd.to_numeric(
                        stream_df["value"], errors="coerce"
                    )

                    # Drop any NaN values that resulted from the conversion
                    stream_df = stream_df.dropna(subset=["value"])

                    if stream_df.empty:
                        continue

                    # Set the time index
                    stream_df = stream_df.set_index(pd.to_datetime(stream_df["time"]))

                    # Resample and handle NaN values
                    stream_df = stream_df.resample("6h")["value"].mean().ffill()

                    # Convert to DataFrame and rename column
                    stream_df = stream_df.to_frame()
                    short_id = stream_id[:6]
                    stream_df = stream_df.rename(
                        columns={"value": f"Stream_{short_id}"}
                    )

                    if first:
                        streams_df = stream_df
                        first = False
                    else:
                        streams_df = pd.merge(
                            streams_df,
                            stream_df,
                            left_index=True,
                            right_index=True,
                            how="outer"
                        )
                except Exception as stream_error:
                    print(f"Error processing stream {stream_id}: {str(stream_error)}")
                    continue

            if not streams_df.empty:
                streams_df = streams_df.reset_index()
                streams_df = streams_df.rename(columns={"index": "time"})

                # Get step function percentage
                step_function_pct = grouped_table_data[grouped_table_data["Brick Class"] == selected_value]["Step Function Percentage"].iloc[0]

                # Create color and dash sequences based on step function status
                colors = []
                dash_patterns = []
                for col in streams_df.columns:
                    if col.startswith("Stream_"):
                        stream_id = col.replace("Stream_", "")
                        full_stream_id = [sid for sid in table_data["Stream ID"] if sid.startswith(stream_id)][0]
                        is_step = table_data[table_data["Stream ID"] == full_stream_id]["Is Step Function"].iloc[0]
                        
                        # Determine if this stream should be highlighted
                        should_highlight = False
                        if step_function_pct != 0 and step_function_pct != 100:
                            if step_function_pct <= 50:
                                should_highlight = is_step
                            else:
                                should_highlight = not is_step
                        
                        colors.append("#808080" if should_highlight else None)
                        dash_patterns.append("solid")#"dash" if should_highlight else "solid")

                plot_component = {
                    "type": "plot",
                    "library": "px",
                    "function": "line",
                    "id": f"brick-class-timeseries-{selected_value}",
                    "kwargs": {
                        "data_frame": streams_df,
                        "x": "time",
                        "y": [col for col in streams_df.columns if col.startswith("Stream_")],
                        "title": f"All {selected_value} Streams (Step Function Percentage: {step_function_pct}%)",
                        "labels": {"time": "Date", "value": "Value"},
                        "color_discrete_sequence": colors,
                        "line_dash_sequence": dash_patterns
                    },
                    "layout_kwargs": {
                        "xaxis_title": "Date",
                        "yaxis_title": "Value",
                        "height": 600,
                        "width": 1200,
                        "showlegend": True,
                    },
                }
                output_results.append([create_plot_component(plot_component)])
            else:
                output_results.append(
                    [
                        html.Div(
                            [
                                html.H4("No Data Available"),
                                html.P(
                                    f"No numeric data available for {selected_value} streams"
                                ),
                            ]
                        )
                    ]
                )

        except Exception as e:
            output_results.append(
                [html.Div([html.H4("Error Processing Data"), html.Pre(str(e))])]
            )
    else:
        # Handle case where conditions aren't met
        error_message = "Invalid configuration: "
        if db is None:
            error_message += "Database connection not provided. "
        if index_column != "Brick Class":
            error_message += (
                f"Expected index_column 'Brick Class' but got '{index_column}'. "
            )

        output_results.append(
            [
                html.Div(
                    [
                        html.H4("Configuration Error"),
                        html.P(error_message),
                        html.Pre(
                            f"Data source: {data_source}\nIndex column: {index_column}"
                        ),
                    ]
                )
            ]
        )

    return output_results


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


def register_plot_callbacks(app: Dash, plot_configs: PlotConfig) -> None:
    """Register callbacks for the plot configurations.

    Args:
        app (Dash): The Dash app instance.
        plot_configs (PlotConfig): The plot configurations.
    """
    action_functions = {
        "process_interaction": process_interaction_action,
        "update_components_based_on_table_selection": update_components_based_on_table_selection_action,
        "update_components_based_on_grouped_table_selection": update_components_based_on_grouped_table_selection_action,
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

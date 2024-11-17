"""
Registers callbacks for interactive analytics components.

This module connects user interactions with the appropriate actions, updating
plots and components dynamically based on user input. It processes the
interactions defined in the plot configurations, setting up triggers and outputs
for interactive and responsive visualisations.
"""

from dash import Dash, no_update
from dash.dependencies import Input, Output

from actions.process_interaction import process_interaction_action
from actions.update_components_based_on_grouped_table_selection import (
    update_components_based_on_grouped_table_selection_action,
)
from actions.update_components_based_on_table_selection import (
    update_components_based_on_table_selection_action,
)
from components.analytics import find_component_by_id
from models.types import PlotConfig


def register_analytics_callbacks(app: Dash, plot_configs: PlotConfig) -> None:
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

    for _, config in plot_configs.items():
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

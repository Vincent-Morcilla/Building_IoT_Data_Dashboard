import copy
from typing import List, Dict, Any
from dash import no_update
from components.plot_generator import (
    create_plot_component,
    find_component_by_id,
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

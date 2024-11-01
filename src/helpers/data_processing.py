from typing import Dict, Any
import pandas as pd


def apply_generic_filters(
    data_frame: pd.DataFrame,
    filters: Dict[str, Any],
    input_mapping: Dict[str, Any],
) -> pd.DataFrame:
    """Apply generic filters to the DataFrame.

    Args:
        data_frame: The DataFrame to filter.
        filters: Filter conditions specified in the interaction configuration.
        input_mapping: Mapping of input keys to actual input values.

    Returns:
        The filtered DataFrame.
    """
    filtered_df = data_frame.copy()

    for column, filter_conditions in filters.items():
        if column not in filtered_df.columns:
            continue

        for operation, value_key in filter_conditions.items():
            if operation == "equals":
                value = input_mapping.get(value_key)
                if value is not None:
                    filtered_df = filtered_df[filtered_df[column] == value]
            elif operation == "in":
                value = input_mapping.get(value_key)
                if value is not None:
                    if isinstance(value, list):
                        filtered_df = filtered_df[filtered_df[column].isin(value)]
                    else:
                        filtered_df = filtered_df[filtered_df[column] == value]
            elif operation == "between":
                between_values = value_key
                start_value = input_mapping.get(between_values.get("start_date"))
                end_value = input_mapping.get(between_values.get("end_date"))
                if start_value and end_value:
                    filtered_df = filtered_df[
                        (filtered_df[column] >= start_value)
                        & (filtered_df[column] <= end_value)
                    ]
            elif operation == "greater_than":
                value = input_mapping.get(value_key)
                if value is not None:
                    filtered_df = filtered_df[filtered_df[column] > value]
            elif operation == "less_than":
                value = input_mapping.get(value_key)
                if value is not None:
                    filtered_df = filtered_df[filtered_df[column] < value]

    return filtered_df


def apply_transformation(
    data_frame: pd.DataFrame,
    transformation: Dict[str, Any],
    input_mapping: Dict[str, Any],
) -> pd.DataFrame:
    """Apply a transformation to the DataFrame.

    Args:
        data_frame: The DataFrame to transform.
        transformation: The transformation instructions.
        input_mapping: Mapping of input keys to actual input values.

    Returns:
        The transformed DataFrame.
    """
    transformed_df = data_frame.copy()
    transformation_type = transformation.get("type")

    if transformation_type == "aggregate":
        groupby_columns = transformation.get("groupby", [])
        aggregations = transformation.get("aggregations", {})
        if groupby_columns and aggregations:
            transformed_df = (
                transformed_df.groupby(groupby_columns)
                .agg(**aggregations)
                .reset_index()
            )
    elif transformation_type == "resample":
        on_column = transformation.get("on")
        frequency_key = transformation.get("frequency")
        frequency = input_mapping.get(frequency_key, "D")
        agg_func = transformation.get("agg_func", "mean")
        if on_column in transformed_df.columns:
            transformed_df[on_column] = pd.to_datetime(
                transformed_df[on_column]
            )  # Ensure datetime
            transformed_df.set_index(on_column, inplace=True)
            transformed_df = (
                transformed_df.resample(frequency).agg(agg_func).reset_index()
            )
    elif transformation_type == "explode":
        columns_to_explode = transformation.get("columns", [])
        for col in columns_to_explode:
            if col in transformed_df.columns:
                transformed_df = transformed_df.explode(col)
            else:
                print(f"Warning: Column '{col}' not found in DataFrame for exploding.")

    return transformed_df

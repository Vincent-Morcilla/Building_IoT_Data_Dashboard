"""
Updates components when a selection is made in a grouped table.

This module defines the action to update components based on the user's selection
in a grouped table. It processes the selected data, retrieves relevant information,
and updates associated plots or components, providing detailed insights and
drill-down capabilities in the app's visualizations.
"""

from typing import List, Dict, Any

from dash import html, no_update
import pandas as pd

from components.plot_generator import (
    create_plot_component,
)
from models.types import PlotConfig


def update_components_based_on_grouped_table_selection_action(
    plot_configs: PlotConfig,
    input_values: List[Any],
    outputs: List[Dict[str, Any]],
    interaction: Dict[str, Any],
    triggers: List[Dict[str, Any]] = None,
) -> List[Any]:
    """
    Update components based on the selected rows in a grouped table.

    Args:
        plot_configs (PlotConfig): The plot configurations.
        input_values (List[Any]): The input values from triggers.
        outputs (List[Dict[str, Any]]): The outputs to update.
        interaction (Dict[str, Any]): The interaction configuration.
        triggers (List[Dict[str, Any]], optional): Trigger configurations.

    Returns:
        List[Any]: The updated outputs for the Dash components.
    """
    output_results = []

    selected_rows = input_values[0] if input_values else []
    if not selected_rows:
        return [no_update] * len(outputs)

    data_source = interaction.get("data_source", {})
    table_data = data_source.get("table_data")
    grouped_table_data = data_source.get("grouped_table_data")
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
                            how="outer",
                        )
                except Exception as stream_error:
                    print(f"Error processing stream {stream_id}: {str(stream_error)}")
                    continue

            if not streams_df.empty:
                streams_df = streams_df.reset_index()
                streams_df = streams_df.rename(columns={"index": "time"})

                # Get step function percentage
                step_function_pct = grouped_table_data[
                    grouped_table_data["Brick Class"] == selected_value
                ]["Step Function Percentage"].iloc[0]

                # Create color and dash sequences based on step function status
                colors = []
                dash_patterns = []
                for col in streams_df.columns:
                    if col.startswith("Stream_"):
                        stream_id = col.replace("Stream_", "")
                        full_stream_id = [
                            sid
                            for sid in table_data["Stream ID"]
                            if sid.startswith(stream_id)
                        ][0]
                        is_step = table_data[table_data["Stream ID"] == full_stream_id][
                            "Is Step Function"
                        ].iloc[0]

                        # Determine if this stream should be highlighted
                        should_highlight = False
                        if step_function_pct not in (0, 100):
                            if step_function_pct <= 50:
                                should_highlight = is_step
                            else:
                                should_highlight = not is_step

                        colors.append("#808080" if should_highlight else None)
                        dash_patterns.append("solid")  # or "dash" if needed

                plot_component = {
                    "type": "plot",
                    "library": "px",
                    "function": "line",
                    "id": f"brick-class-timeseries-{selected_value}",
                    "kwargs": {
                        "data_frame": streams_df,
                        "x": "time",
                        "y": [
                            col
                            for col in streams_df.columns
                            if col.startswith("Stream_")
                        ],
                        "title": (
                            f"All {selected_value} Streams "
                            f"(Step Function Percentage: {step_function_pct}%)"
                        ),
                        "labels": {"time": "Date", "value": "Value"},
                        "color_discrete_sequence": colors,
                        "line_dash_sequence": dash_patterns,
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

        except Exception as error:
            output_results.append(
                [html.Div([html.H4("Error Processing Data"), html.Pre(str(error))])]
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

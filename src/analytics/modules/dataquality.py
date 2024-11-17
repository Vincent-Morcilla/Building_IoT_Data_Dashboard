"""
This module performs data quality analysis on sensor data.

It includes functions for preprocessing sensor data, analyzing gaps,
detecting outliers, and generating summary statistics and visualizations.
"""

import datetime

import numpy as np
import pandas as pd

from analytics.dbmgr import DBManager
from models.types import PlotConfig  # only imported for type hinting


def _detect_step_function_behavior(
    values, percentage_threshold=0.1, unique_values_threshold=5
):
    """
    Detect if a time series behaves like a step function.

    Args:
        values (np.array): Array of sensor values
        percentage_threshold (float): Threshold for relative change (default: 0.1)
        unique_values_threshold (int): Threshold for unique values (default: 5)

    Returns:
        dict: Dictionary containing step function metrics
    """
    if len(values) < 2:
        return {
            "percentage_flat": None,
            "unique_values_count": None,
            "is_step_function": None,
        }

    # Calculate differences between consecutive values
    differences = np.diff(values)

    # Handle zero and near-zero values more robustly
    prev_values = values[:-1]
    relative_differences = np.zeros_like(differences, dtype=float)

    # Mask for non-zero values
    non_zero_mask = np.abs(prev_values) > 1e-10

    # Calculate relative differences only for non-zero values
    relative_differences[non_zero_mask] = (
        np.abs(differences[non_zero_mask] / prev_values[non_zero_mask]) * 100
    )

    # For zero values, use absolute differences
    relative_differences[~non_zero_mask] = np.abs(differences[~non_zero_mask])

    # Calculate metrics
    flat_regions = relative_differences < percentage_threshold
    percentage_flat = np.sum(flat_regions) / len(differences) * 100
    unique_values_count = len(np.unique(values))
    is_step_function = (percentage_flat > 90) and (
        unique_values_count <= unique_values_threshold
    )

    return {
        "percentage_flat": percentage_flat,
        "unique_values_count": unique_values_count,
        "is_step_function": is_step_function,
    }


def _preprocess_to_sensor_rows(db: DBManager):
    """Preprocess raw sensor data into a standardized DataFrame format.

    This function processes all streams from the database and computes initial metrics including:
    - Basic statistics (mean, min, max)
    - Missing value counts
    - Zero value counts
    - Step function detection
    - Outlier detection
    - Time range information

    Args:
        db (DBManager): Database manager instance containing sensor streams

    Returns:
        pd.DataFrame: Preprocessed DataFrame containing rows for each sensor

    Returns empty dict if no streams are found in the database.
    """
    sensor_data = []
    all_streams = db.get_all_streams()

    if all_streams == {}:
        return {}

    for stream_id, df in all_streams.items():
        try:
            label = db.get_label(stream_id)

            if df.empty:
                continue

            values = df["value"].values
            step_info = _detect_step_function_behavior(values)

            row = {
                "stream_id": stream_id,
                "Label": label,
                "Timestamps": df["time"],
                "Values": values,
                "Deduced_Granularity": _deduce_granularity(df["time"]),
                "Value_Count": len(df),
                "Outliers": _detect_outliers(df["value"].values)[0],
                "Missing": df["value"].isna().sum(),
                "Zeros": (df["value"] == 0).sum(),
                "Start_Timestamp": df["time"].min(),
                "End_Timestamp": df["time"].max(),
                "Sensor_Mean": df["value"].mean(),
                "Sensor_Min": df["value"].min(),
                "Sensor_Max": df["value"].max(),
                "Percentage_Flat_Regions": step_info["percentage_flat"],
                "Unique_Values_Count": step_info["unique_values_count"],
                "Is_Step_Function": step_info["is_step_function"],
            }
            sensor_data.append(row)
        except KeyError:
            continue

    return pd.DataFrame(sensor_data)


def _profile_groups(df):
    """
    Calculate group statistics for sensor data and flag outlier sensors.

    Args:
        df (pd.DataFrame): Preprocessed sensor data.

    Returns:
        pd.DataFrame: Sensor data with added group statistics and flags.
    """
    # Create a copy to avoid modifying the original
    result = df.copy()

    # Initialize columns
    result["Group_Mean"] = np.nan
    result["Group_Std"] = np.nan
    result["Flagged For Removal"] = 0  # Initialize with zeros

    # Group by Label
    for _, group in df.groupby("Label"):
        # Calculate group statistics
        group_mean = group["Sensor_Mean"].mean()
        group_std = group["Sensor_Mean"].std()

        # Store group statistics
        result.loc[group.index, "Group_Mean"] = group_mean
        result.loc[group.index, "Group_Std"] = group_std

        # Skip flagging if group_std is 0 (usually setpoints)
        if group_std == 0:
            continue

        # Calculate limits
        lower_limit = group_mean - 3 * group_std
        upper_limit = group_mean + 3 * group_std

        # Flag sensors outside the limits
        flags = (group["Sensor_Mean"] < lower_limit) | (
            group["Sensor_Mean"] > upper_limit
        )
        result.loc[group.index, "Flagged For Removal"] = flags.astype(int)

    return result


def _analyse_sensor_gaps(df):
    """
    Analyze gaps in sensor data.

    Args:
        df (pd.DataFrame): Preprocessed sensor data.

    Returns:
        pd.DataFrame: Gap analysis results.
    """

    def process_row(row):
        timestamps = row["Timestamps"]
        granularity = row["Deduced_Granularity"]

        if len(timestamps) < 2:
            return pd.Series(
                {
                    "Small_Gap_Count": 0,
                    "Medium_Gap_Count": 0,
                    "Large_Gap_Count": 0,
                    "Total_Gaps": 0,
                    "Gap_Percentage": 0,
                    "Total_Gap_Size_Seconds": 0,
                }
            )

        granularity_parts = str(granularity).split()
        time_granularity = int(granularity_parts[0])
        time_unit = granularity_parts[1]
        granularity_in_seconds = time_granularity * {"minutes": 60, "hours": 3600}.get(
            time_unit, 86400
        )

        time_diffs = np.diff(timestamps.astype(int)) / 1e9  # Convert to seconds
        expected_diff = granularity_in_seconds
        normalised_diffs = time_diffs / expected_diff

        small_gap = np.sum((normalised_diffs > 1.5) & (normalised_diffs <= 3))
        medium_gap = np.sum((normalised_diffs > 3) & (normalised_diffs <= 6))
        large_gap = np.sum(normalised_diffs > 6)
        total_gaps = small_gap + medium_gap + large_gap
        time_delta_seconds = (timestamps.iloc[-1] - timestamps.iloc[0]).total_seconds()
        total_gap_intervals = sum(diff - 1 for diff in normalised_diffs if diff > 1.5)
        total_gap_size_seconds = total_gap_intervals * granularity_in_seconds
        gap_percentage = (
            total_gap_size_seconds / time_delta_seconds
            if time_delta_seconds > 0
            else 0.0
        )

        return pd.Series(
            {
                "Small_Gap_Count": small_gap,
                "Medium_Gap_Count": medium_gap,
                "Large_Gap_Count": large_gap,
                "Total_Gaps": total_gaps,
                "Gap_Percentage": gap_percentage,
                "Total_Gap_Size_Seconds": total_gap_size_seconds,
            }
        )

    return df.apply(process_row, axis=1)


def _prepare_data_quality_df(df: pd.DataFrame) -> pd.DataFrame:
    """Prepare data quality DataFrame with standardized column names and formats.

    Args:
        df (pd.DataFrame): Raw data quality metrics DataFrame

    Returns:
        pd.DataFrame: Processed DataFrame with standardized columns and formats
    """
    # Debug print to see what columns we actually have
    # print("Available columns:", df.columns.tolist())

    # Create the display DataFrame with proper column names
    data_quality_df = pd.DataFrame(
        {
            "Stream ID": df["stream_id"],
            # "Label": df["Label"],  # Keep Label for reference
            "Brick Class": df["Label"],
            "Sample Rate": df["Deduced_Granularity"],
            "Samples": df["Value_Count"],
            "Outliers": df["Outliers"],
            "Missing": df["Missing"],
            "Zeros": df["Zeros"],
            "Small Gaps": df.get("Small_Gap_Count", 0),
            "Medium Gaps": df.get("Medium_Gap_Count", 0),
            "Large Gaps": df.get("Large_Gap_Count", 0),
            "Total Gaps": df.get("Total_Gaps", 0),
            "Gap Percentage": df.get("Gap_Percentage", 0).round(2),
            "Total Gap Size (s)": df.get("Total_Gap_Size_Seconds", 0),
            "Group Mean": df.get("Group_Mean", 0).round(2),
            "Group Std": df.get("Group_Std", 0).round(2),
            "Sensor Mean": df["Sensor_Mean"].round(2),
            "Sensor Min": df["Sensor_Min"],
            "Sensor Max": df["Sensor_Max"],
            "Flagged For Removal": df["Flagged For Removal"],
            "Start Timestamp": df["Start_Timestamp"],
            "End Timestamp": df["End_Timestamp"],
            "Flat Regions %": df["Percentage_Flat_Regions"].round(2),
            "Unique Values": df["Unique_Values_Count"],
            "Is Step Function": df["Is_Step_Function"],
        }
    )

    data_quality_df.sort_values(["Brick Class", "Stream ID"], inplace=True)

    return data_quality_df


def _create_summary_table(data_quality_df):
    """Create summary statistics table grouped by sensor class.

    Args:
        data_quality_df (pd.DataFrame): DataFrame containing data quality metrics

    Returns:
        pd.DataFrame: Summary table with statistics grouped by sensor class
    """
    summary_table = (
        data_quality_df.groupby("Brick Class")
        .agg(
            {
                "Stream ID": "count",
                "Sample Rate": lambda x: x.mode()[0],
                "Start Timestamp": "min",
                "End Timestamp": "max",
                "Samples": "sum",
                "Outliers": "sum",
                "Missing": "sum",
                "Zeros": "sum",
                "Small Gaps": "sum",
                "Medium Gaps": "sum",
                "Large Gaps": "sum",
                "Total Gaps": "sum",
                "Group Mean": "first",
                "Group Std": "first",
                "Total Gap Size (s)": "sum",
                "Flagged For Removal": "sum",
                "Is Step Function": lambda x: (x.sum() / len(x) * 100).round(
                    2
                ),  # Add this line
            }
        )
        .reset_index()
    )

    # Calculate gap percentage for the group
    summary_table["Gap Percentage"] = (
        summary_table["Total Gap Size (s)"]
        / (
            summary_table["End Timestamp"] - summary_table["Start Timestamp"]
        ).dt.total_seconds()
        * 100
    ).round(2)
    summary_table = summary_table.rename(
        columns={
            "Stream ID": "Number of Streams",
            "Is Step Function": "Step Function Percentage",
        }
    )

    summary_table.sort_values(["Brick Class"], inplace=True)

    return summary_table


def _get_data_quality_overview(data_quality_df):
    """Generate overview visualizations and statistics for data quality analysis.

    Args:
        data_quality_df (pd.DataFrame): DataFrame containing data quality metrics for all sensors

    Returns:
        dict: Dictionary containing:
            - tables: List of overall statistics tables
            - timeline: Sensor timeline visualization configuration
    """
    # Create a 'has_outliers' column
    data_quality_df["has_outliers"] = data_quality_df["Outliers"] > 0

    # Create stats DataFrame
    stats_df = pd.DataFrame(
        {
            "Metric": [
                "Total Data Sources",
                "Data Sources with Outliers",
                "Data Sources with Gaps",
                "Total Small Gaps",
                "Total Medium Gaps",
                "Total Large Gaps",
                "Average Gap Percentage",
                "Data Sources Flagged For Removal",
                "Step Function Data Sources",
            ],
            "Value": [
                len(data_quality_df),
                data_quality_df["has_outliers"].sum(),
                (data_quality_df["Total Gaps"] > 0).sum(),
                data_quality_df["Small Gaps"].sum(),
                data_quality_df["Medium Gaps"].sum(),
                data_quality_df["Large Gaps"].sum(),
                f"{data_quality_df['Gap Percentage'].mean():.2%}",
                data_quality_df["Flagged For Removal"].sum(),
                data_quality_df["Is Step Function"].sum(),
            ],
        }
    )

    # Table for overall statistics
    overall_stats = {
        "title": "Overall Data Quality Statistics",
        "columns": ["Metric", "Value"],
        "rows": ["Metric", "Value"],
        "data_source": "DataQuality_Overview",
        "filter": None,
        "dataframe": stats_df,
    }

    # Add timeline configuration
    timeline_data = (
        data_quality_df.groupby("Brick Class")
        .agg({"Start Timestamp": "min", "End Timestamp": "max", "Stream ID": "count"})
        .reset_index()
    )

    # Ensure timestamps are properly converted to datetime
    timeline_data["Start Timestamp"] = pd.to_datetime(
        timeline_data["Start Timestamp"], utc=True
    )
    timeline_data["End Timestamp"] = pd.to_datetime(
        timeline_data["End Timestamp"], utc=True
    )

    # Convert to local timezone and remove timezone info to avoid mixing aware/naive datetimes
    timeline_data["Start Timestamp"] = timeline_data["Start Timestamp"].dt.tz_localize(
        None
    )
    timeline_data["End Timestamp"] = timeline_data["End Timestamp"].dt.tz_localize(None)

    # Sort the data by start Brick Class
    timeline_data = timeline_data.sort_values("Brick Class", ascending=False)

    sensor_timeline = {
        "title": "Sensor Time Coverage by Brick Class",
        "type": "Timeline",
        "x-axis": ["Start Timestamp", "End Timestamp"],
        "y-axis": "Brick Class",
        "size": "Stream ID",
        "x-axis_label": "Time Range",
        "y-axis_label": "Sensor Label",
        "dataframe": timeline_data,
    }

    return {
        # "pie_charts": [outliers_pie, gaps_pie],
        "tables": [overall_stats],
        # "histogram": stream_histogram,
        "timeline": sensor_timeline,
    }


def _deduce_granularity(timestamps):
    """
    Deduce the granularity of timestamps.

    Args:
        timestamps (pd.Series): Series of timestamps.

    Returns:
        str: Deduced granularity as a string.
    """
    time_diffs = np.diff(timestamps).astype("timedelta64[s]").astype(int)  # in seconds
    if len(time_diffs) == 0:
        return None

    granularity = pd.Series(time_diffs).mode()[
        0
    ]  # Most common time interval in seconds

    # Upgrade granularity
    if granularity % 86400 == 0:  # 86400 seconds = 1 day
        return f"{granularity // 86400} days"
    if granularity % 3600 == 0:  # 3600 seconds = 1 hour
        return f"{granularity // 3600} hours"
    if 0 <= granularity % 60 < 5:  # 60 seconds = 1 minute
        return f"{granularity // 60} minutes"
    if granularity % 60 > 55:
        return f"{1 + granularity // 60} minutes"
    return f"{granularity} seconds"


def _detect_outliers(values):
    """
    Detect outliers in a series of values.

    Args:
        values (np.array): Array of values.

    Returns:
        tuple: Number of outliers and (lower bound, upper bound).
    """
    q1 = np.percentile(values, 25)
    q3 = np.percentile(values, 75)
    iqr = q3 - q1
    lower_bound = q1 - (1.5 * iqr)
    upper_bound = q3 + (1.5 * iqr)
    outliers = np.sum((values < lower_bound) | (values > upper_bound))
    return outliers, (lower_bound, upper_bound)


def _build_components(df: pd.DataFrame, stream_id: str, title: str) -> list:
    """Build visualization components for a single sensor stream.

    Args:
        stream_df (pd.DataFrame): DataFrame containing stream time series data
        stream_id (str): Identifier for the stream
        title (str): Title for the visualization

    Returns:
        dict: Dictionary containing plot configurations and components
    """
    return [
        {
            "type": "plot",
            "library": "px",
            "function": "line",
            "id": f"dataquality-line-plot-{stream_id}",
            "kwargs": {
                "data_frame": df,
                "x": "Date",
                "y": df.columns,
            },
            "layout_kwargs": {
                "title": {
                    "text": title,
                    "font": {"size": 20},
                    "x": 0.5,
                    "xanchor": "center",
                    "subtitle": {
                        "text": f"Stream ID: {stream_id}",
                        "font": {"size": 12},
                    },
                },
                "autosize": True,
                "font_color": "black",
                "plot_bgcolor": "white",
                "height": 500,
                "margin": {
                    "t": 100,
                },
                "showlegend": False,
                "xaxis": {
                    "mirror": True,
                    "ticks": "outside",
                    "showline": True,
                    "linecolor": "black",
                    "gridcolor": "lightgrey",
                },
                "yaxis": {
                    "mirror": True,
                    "ticks": "outside",
                    "showline": True,
                    "linecolor": "black",
                    "gridcolor": "lightgrey",
                },
            },
        },
    ]


def _generate_green_scale(n_colors):
    """Generate a green color scale with the specified number of colors."""

    # Create evenly spaced values between 0 and 1
    values = np.linspace(0, 1, n_colors)

    # Define our green scale endpoints
    dark_green = np.array([30, 76, 29])  # #1e4c1d
    theme_green = np.array([60, 150, 57])  # #3c9639
    light_green = np.array([184, 228, 183])  # #b8e4b7

    colors = []
    for v in values:
        if v <= 0.5:
            # Interpolate between dark and theme green
            rgb = dark_green + (2 * v) * (theme_green - dark_green)
        else:
            # Interpolate between theme and light green
            v = v - 0.5
            rgb = theme_green + (2 * v) * (light_green - theme_green)

        # Convert to hex
        hex_color = f"#{int(rgb[0]):02x}{int(rgb[1]):02x}{int(rgb[2]):02x}"
        colors.append(hex_color)

    return colors


def _get_column_type(value):
    """Determine the column type based on Python type."""
    python_type = type(value)
    if python_type == str:
        return "text"
    if python_type in (int, float):
        return "numeric"
    if python_type == bool:
        return "boolean"
    if python_type in (pd.Timestamp, datetime.datetime):
        return "datetime"
    return "any"


def run(db: DBManager) -> PlotConfig:
    """Run data quality analysis and generate visualization configurations.

    This function processes sensor data through multiple analysis steps:
    1. Preprocesses raw sensor data
    2. Analyzes gaps and outliers
    3. Profiles sensor groups
    4. Generates visualization configurations

    Args:
        db (DBManager): Database manager instance containing sensor data

    Returns:
        PlotConfig: A nested dictionary containing three main sections:
            - ('DataQuality', 'Overview'): Overall statistics and visualizations
                - Tables
                - Pie charts (outliers, step functions)
                - Timeline visualization
            - ('DataQuality', 'ByClass'): Analysis grouped by sensor class
                - Interactive timeseries plots
                - Summary tables
            - ('DataQuality', 'ByStream'): Individual stream analysis
                - Detailed metrics table
                - Interactive timeseries visualization
    """
    df = _preprocess_to_sensor_rows(db)

    # Return empty result if no data
    if df.empty:
        return {}

    df = _profile_groups(df)

    gap_analysis_results = _analyse_sensor_gaps(df)

    df = pd.concat([df, gap_analysis_results], axis=1)

    data_quality_df = _prepare_data_quality_df(df)
    n_classes = len(data_quality_df["Brick Class"].unique())

    summary_table_df = _create_summary_table(data_quality_df)
    overview_data = _get_data_quality_overview(data_quality_df)

    # Build timeseries data dictionary FIRST
    timeseries_data_dict = {}
    for _, row in data_quality_df.iterrows():
        stream_df = db.get_stream(row["Stream ID"])
        stream_df = stream_df.pivot(index="time", columns="brick_class", values="value")
        stream_df = stream_df.resample("1h").mean()

        stream_df = stream_df.reset_index()
        stream_df.rename(columns={"time": "Date"}, inplace=True)
        stream_df["Date"] = stream_df["Date"].astype("datetime64[ns]")

        stream_type = row["Brick Class"].replace("_", " ")
        stream_id = row["Stream ID"]
        title = f"{stream_type} Timeseries Data"

        timeseries_data_dict[stream_id] = _build_components(stream_df, stream_id, title)

    # pylint: disable=singleton-comparison
    plot_config = {
        ("DataQuality", "Overview"): {
            "components": [
                {
                    "type": "table",
                    "dataframe": overview_data["tables"][0]["dataframe"],
                    "id": "data-quality-overall-stats-table",
                    "title": "Overall Data Quality Statistics",
                    "title_element": "H5",
                    "kwargs": {
                        "columns": [
                            {"name": i, "id": i, "type": "text"}
                            for i in ["Metric", "Value"]
                        ],
                        "export_format": "csv",
                        "fixed_rows": {"headers": True},
                        "style_header": {
                            "fontWeight": "bold",
                            "backgroundColor": "#3c9639",
                            "color": "white",
                            "textAlign": "center",
                            "whiteSpace": "normal",  # Allow text wrapping in headers
                            "height": "auto",  # Adjust height automatically
                            "minWidth": "100px",  # Minimum width for columns
                        },
                        "style_cell": {
                            "textAlign": "center",
                            "padding": "10px",
                            "whiteSpace": "normal",  # Allow text wrapping in cells
                            "height": "auto",
                            "minWidth": "100px",  # Minimum width for columns
                            "maxWidth": "180px",  # Maximum width for columns
                            "overflow": "hidden",
                            "textOverflow": "ellipsis",
                        },
                        "style_table": {
                            "height": 600,
                            "overflowX": "auto",
                            "minWidth": "100%",  # Table takes full width
                        },
                        "style_data_conditional": [
                            {
                                "if": {"row_index": "odd"},
                                "backgroundColor": "#ddf2dc",
                            }
                        ],
                    },
                    "css": {
                        "marginTop": "0%",
                        "marginBottom": "0%",
                    },
                },
                {
                    "type": "plot",
                    "library": "go",
                    "function": "Figure",
                    "id": "data-quality-outliers-pie",
                    "data_frame": pd.DataFrame(
                        {
                            "Status": ["No Outliers", "Has Outliers"],  # Better labels
                            "Count": [
                                len(
                                    data_quality_df[
                                        data_quality_df["has_outliers"] == False
                                    ]
                                ),
                                len(
                                    data_quality_df[
                                        data_quality_df["has_outliers"] == True
                                    ]
                                ),
                            ],
                        }
                    ),
                    "trace_type": "Pie",
                    "data_mappings": {"labels": "Status", "values": "Count"},
                    "kwargs": {
                        "textinfo": "percent+label",
                        "textposition": "inside",
                        "showlegend": False,
                        "marker": {
                            "colors": [
                                "#2d722c",
                                "#3c9639",
                            ]  # Just two colors for binary
                        },
                    },
                    "layout_kwargs": {
                        "title": {
                            "text": "Data Sources with Outliers",
                            "font_color": "black",
                            "x": 0.5,
                            "xanchor": "center",
                        },
                        "margin": {"t": 50, "b": 50, "l": 50, "r": 50},
                    },
                    "css": {
                        "width": "50%",
                        "display": "inline-block",
                    },
                },
                {
                    "type": "plot",
                    "library": "go",
                    "function": "Figure",
                    "id": "data-quality-outliers-by-class-pie",
                    "data_frame": summary_table_df,
                    "trace_type": "Pie",
                    "data_mappings": {"labels": "Brick Class", "values": "Outliers"},
                    "kwargs": {
                        "textinfo": "percent+label",
                        "textposition": "inside",
                        "showlegend": False,
                        "marker": {"colors": _generate_green_scale(n_classes)},
                    },
                    "layout_kwargs": {
                        "title": {
                            "text": "Outlier Percentage by Class",
                            "font_color": "black",
                            "x": 0.5,
                            "xanchor": "center",
                        },
                        "margin": {"t": 50, "b": 50, "l": 50, "r": 50},
                    },
                    "css": {
                        "width": "50%",
                        "display": "inline-block",
                    },
                },
                {
                    "type": "plot",
                    "library": "go",
                    "function": "Figure",
                    "id": "data-quality-flagged-pie",
                    "data_frame": pd.DataFrame(
                        {
                            "Status": ["Not Flagged", "Flagged"],
                            "Count": [
                                len(
                                    data_quality_df[
                                        data_quality_df["Flagged For Removal"] == 0
                                    ]
                                ),
                                len(
                                    data_quality_df[
                                        data_quality_df["Flagged For Removal"] == 1
                                    ]
                                ),
                            ],
                        }
                    ),
                    "trace_type": "Pie",
                    "data_mappings": {"labels": "Status", "values": "Count"},
                    "kwargs": {
                        "textinfo": "percent+label",
                        "textposition": "inside",
                        "showlegend": False,
                        "marker": {"colors": ["#2d722c", "#3c9639"]},
                    },
                    "layout_kwargs": {
                        "title": {
                            "text": "Flagged Data Sources Distribution",
                            "font_color": "black",
                            "x": 0.5,
                            "xanchor": "center",
                        },
                        "margin": {"t": 50, "b": 50, "l": 50, "r": 50},
                    },
                    "css": {
                        "width": "50%",
                        "display": "inline-block",
                    },
                },
                {
                    "type": "plot",
                    "library": "go",
                    "function": "Figure",
                    "id": "data-quality-stepfunction-pie",
                    "data_frame": pd.DataFrame(
                        {
                            "Status": ["Not Step Function", "Step Function"],
                            "Count": [
                                len(
                                    data_quality_df[
                                        data_quality_df["Is Step Function"] == False
                                    ]
                                ),
                                len(
                                    data_quality_df[
                                        data_quality_df["Is Step Function"] == True
                                    ]
                                ),
                            ],
                        }
                    ),
                    "trace_type": "Pie",
                    "data_mappings": {"labels": "Status", "values": "Count"},
                    "kwargs": {
                        "textinfo": "percent+label",
                        "textposition": "inside",
                        "showlegend": False,
                        "marker": {"colors": ["#2d722c", "#3c9639"]},
                    },
                    "layout_kwargs": {
                        "title": {
                            "text": "Step Function Distribution",
                            "font_color": "black",
                            "x": 0.5,
                            "xanchor": "center",
                        },
                        "margin": {"t": 50, "b": 50, "l": 50, "r": 50},
                    },
                    "css": {
                        "width": "50%",
                        "display": "inline-block",
                    },
                },
                {
                    "type": "plot",
                    "library": "go",
                    "function": "Figure",
                    "id": "data-quality-stream-histogram",
                    "data_frame": data_quality_df.groupby("Brick Class")
                    .size()
                    .reset_index(name="Stream_Count"),
                    "trace_type": "Bar",  # Using Bar instead of Histogram for this case
                    "data_mappings": {"x": "Brick Class", "y": "Stream_Count"},
                    "kwargs": {"textposition": "auto", "marker": {"color": "#3c9639"}},
                    "layout_kwargs": {
                        "title": {
                            "text": "Stream Counts by Brick Class",
                            "font_color": "black",
                            "x": 0.5,
                            "xanchor": "center",
                        },
                        "font_color": "black",
                        "plot_bgcolor": "white",
                        "xaxis": {
                            "title": "Class Type",
                            "tickangle": 45,  # Slant the labels by 45 degrees
                            "tickfont": {"size": 10},  # Reduce font size
                            "tickmode": "auto",
                            "automargin": True,  # Automatically adjust margins
                            "linecolor": "black",
                            "mirror": True,
                        },
                        "yaxis": {
                            "mirror": True,
                            "ticks": "outside",
                            "showline": True,
                            "linecolor": "black",
                            "gridcolor": "lightgrey",
                        },
                        "yaxis_title": "Number of Streams",
                        "showlegend": False,
                        "bargap": 0.2,
                        "height": 600,
                        # "width": 1200,
                        "margin": {"t": 50, "b": 50, "l": 50, "r": 50},
                    },
                    "css": {
                        "display": "block",
                        "marginTop": "5%",
                    },
                },
                {
                    "type": "plot",
                    "library": "go",
                    "function": "Figure",
                    "id": "data-quality-timeline",
                    "data_frame": overview_data["timeline"]["dataframe"],
                    "trace_type": "Bar",
                    "data_mappings": {
                        "base": "Start Timestamp",
                        "x": "End Timestamp",
                        "y": "Brick Class",
                        "customdata": "Stream ID",
                    },
                    "kwargs": {"orientation": "h", "marker": {"color": "#3c9639"}},
                    "layout_kwargs": {
                        "title": {
                            "text": overview_data["timeline"]["title"],
                            "x": 0.5,
                            "xanchor": "center",
                            "font_color": "black",
                        },
                        "font_color": "black",
                        "plot_bgcolor": "white",
                        "xaxis": {
                            "title": overview_data["timeline"]["x-axis_label"],
                            "type": "date",
                            "tickformat": "%Y-%m-%d",
                            "tickfont": {"size": 10},
                            "automargin": True,
                            "linecolor": "black",
                            "mirror": True,
                            "range": [
                                overview_data["timeline"]["dataframe"][
                                    "Start Timestamp"
                                ].min(),
                                overview_data["timeline"]["dataframe"][
                                    "End Timestamp"
                                ].max(),
                            ],
                        },
                        "yaxis": {
                            "tickfont": {"size": 8},
                            "automargin": True,
                            "linecolor": "black",
                            "mirror": True,
                            "side": "right",
                        },
                        "showlegend": False,
                        "height": 600,
                        # "width": 1000,
                        "margin": {"t": 50, "b": 50, "l": 50, "r": 50},
                        "hovermode": "closest",
                        "hoverlabel": {
                            "bgcolor": "white",
                            "font_size": 12,
                        },
                        "shapes": [
                            {
                                "type": "line",
                                "x0": overview_data["timeline"]["dataframe"][
                                    "Start Timestamp"
                                ].min(),
                                "x1": overview_data["timeline"]["dataframe"][
                                    "End Timestamp"
                                ].max(),
                                "y0": i,
                                "y1": i,
                                "line": {
                                    "color": "lightgrey",
                                    "width": 1,
                                    "dash": "dot",
                                },
                            }
                            for i in range(len(overview_data["timeline"]["dataframe"]))
                        ],
                    },
                    "css": {
                        "display": "block",
                        "marginTop": "5%",
                    },
                },
            ],
        },
        ("DataQuality", "ByClass"): {
            "components": [
                # Plot placeholder first
                {
                    "type": "placeholder",
                    "id": "brick-class-timeseries-placeholder",
                },
                # Optional separator
                {
                    "type": "separator",
                    "style": {"margin": "20px 0"},
                },
                # Table second
                {
                    "type": "UI",
                    "element": "DataTable",
                    "id": "data-quality-by-class-table",
                    "kwargs": {
                        "columns": [
                            {
                                "name": i,
                                "id": i,
                                "type": (
                                    "text"
                                    if isinstance(summary_table_df[i].iloc[0], str)
                                    else (
                                        "datetime"
                                        if isinstance(
                                            summary_table_df[i].iloc[0],
                                            (pd.Timestamp, datetime.datetime),
                                        )
                                        else "numeric"
                                    )
                                ),
                            }
                            for i in summary_table_df.columns
                        ],
                        "data": summary_table_df.to_dict("records"),
                        "export_format": "csv",
                        "filter_action": (
                            "native" if len(summary_table_df) > 20 else "none"
                        ),
                        "fixed_rows": {"headers": True},
                        "row_selectable": "single",
                        "selected_rows": [0],
                        "sort_action": "native",
                        "sort_mode": "multi",
                        "style_header": {
                            "fontWeight": "bold",
                            "backgroundColor": "#3c9639",
                            "color": "white",
                            "textAlign": "center",
                            "whiteSpace": "normal",
                            "height": "auto",
                            "minWidth": "100px",
                        },
                        "style_filter": {
                            "backgroundColor": "#3c9639",
                            "color": "white !important",  # Force white color with !important
                        },
                        "style_cell": {
                            "textAlign": "center",
                            "padding": "10px",
                            "whiteSpace": "normal",  # Allow text wrapping in cells
                            "height": "auto",
                            "minWidth": "100px",  # Minimum width for columns
                            "maxWidth": "350px",  # Maximum width for columns
                            "overflow": "hidden",
                            "textOverflow": "ellipsis",
                        },
                        "style_table": {
                            "height": 1500,
                            "overflowX": "auto",
                            "minWidth": "100%",  # Table takes full width
                        },
                        "style_data": {
                            "textAlign": "center",
                            "cursor": "pointer",
                        },
                        "css": [
                            {
                                "selector": "input[type=radio]",
                                "rule": """
                                    appearance: none;
                                    -webkit-appearance: none;
                                    border: 2px solid #3c9639;
                                    border-radius: 50%;
                                    width: 6px; 
                                    height: 6px;
                                    transition: 0.2s all linear;
                                    outline: none;
                                    margin-right: 5px;
                                    position: relative;
                                    top: 4px;
                                    vertical-align: middle;
                                """,
                            },
                            {
                                "selector": "input[type=radio]:checked",
                                "rule": """
                                    border: 2px solid #3c9639;
                                    background-color: #3c9639;  
                                """,
                            },
                            {
                                "selector": ".dash-filter input",
                                "rule": "color: white !important;",
                            },
                            {
                                "selector": ".dash-table-container td:first-child",
                                "rule": "white-space: nowrap !important;",
                            },
                        ],
                        "style_data_conditional": [
                            {
                                "if": {"row_index": "odd"},
                                "backgroundColor": "#ddf2dc",
                            }
                        ],
                        "tooltip_data": [
                            {
                                column: {"value": str(value), "type": "markdown"}
                                for column, value in row.items()
                            }
                            for row in df.to_dict("records")
                        ],
                        "tooltip_duration": None,
                    },
                },
            ],
            "interactions": [
                {
                    "triggers": [
                        {
                            "component_id": "data-quality-by-class-table",
                            "component_property": "selected_rows",
                            "input_key": "selected_rows",
                        },
                    ],
                    "outputs": [
                        {
                            "component_id": "brick-class-timeseries-placeholder",
                            "component_property": "children",
                        },
                    ],
                    "action": "update_components_based_on_grouped_table_selection",
                    "data_source": {
                        "table_data": data_quality_df,
                        "grouped_table_data": summary_table_df,
                        "db": db,
                        "include_data_dict_in_download": False,
                    },
                    "index_column": "Brick Class",
                }
            ],
        },
        ("DataQuality", "ByStream"): {
            "components": [
                {
                    "type": "placeholder",
                    "id": "dataquality-timeseries-placeholder",
                },
                {
                    "type": "separator",
                    "style": {"margin": "20px 0"},
                },
                {
                    "type": "UI",
                    "element": "DataTable",
                    "id": "data-quality-metrics-table",
                    "kwargs": {
                        "columns": [
                            {
                                "name": col,
                                "id": col,
                                "type": (
                                    "text"
                                    if isinstance(data_quality_df[col].iloc[0], str)
                                    else (
                                        "datetime"
                                        if isinstance(
                                            data_quality_df[col].iloc[0],
                                            (pd.Timestamp, datetime.datetime),
                                        )
                                        else "numeric"
                                    )
                                ),
                            }
                            for col in [
                                "Stream ID",
                                "Brick Class",
                                "Sample Rate",
                                "Samples",
                                "Outliers",
                                "Missing",
                                "Zeros",
                                "Small Gaps",
                                "Medium Gaps",
                                "Large Gaps",
                                "Total Gaps",
                                "Gap Percentage",
                                "Total Gap Size (s)",
                                "Group Mean",
                                "Group Std",
                                "Sensor Mean",
                                "Sensor Min",
                                "Sensor Max",
                                "Flagged For Removal",
                                "Start Timestamp",
                                "End Timestamp",
                                "Flat Regions %",
                                "Unique Values",
                                "Is Step Function",
                            ]
                        ],
                        "data": data_quality_df.to_dict("records"),
                        "export_format": "csv",
                        "filter_action": (
                            "native" if len(data_quality_df) > 20 else "none"
                        ),
                        "fixed_rows": {"headers": True},
                        "row_selectable": "single",
                        "selected_rows": [0],
                        "sort_action": "native",
                        "sort_mode": "multi",
                        "style_data_conditional": [
                            {
                                "if": {"row_index": "odd"},
                                "backgroundColor": "#ddf2dc",
                            },
                            {
                                "if": {"filter_query": "{Flagged For Removal} gt 0"},
                                "backgroundColor": "#ffcccb",  # Light red background for flagged sensors
                            },
                        ],
                        "style_header": {
                            "fontWeight": "bold",
                            "backgroundColor": "#3c9639",
                            "color": "white",
                            "textAlign": "center",
                            "whiteSpace": "normal",  # Allow text wrapping in headers
                            "height": "auto",  # Adjust height automatically
                            "minWidth": "100px",  # Minimum width for columns
                        },
                        "style_cell": {
                            "textAlign": "center",
                            "padding": "10px",
                            "whiteSpace": "normal",  # Allow text wrapping in cells
                            "height": "auto",
                            "minWidth": "100px",  # Minimum width for columns
                            "maxWidth": "350px",  # Maximum width for columns
                            "overflow": "hidden",
                            "textOverflow": "ellipsis",
                        },
                        "style_table": {
                            "height": 2000,
                            "overflowX": "auto",
                            "minWidth": "100%",  # Table takes full width
                        },
                        "style_data": {"textAlign": "center"},  # Center all cell data
                        "css": [
                            {
                                "selector": "input[type=radio]",
                                "rule": """
                                    appearance: none;
                                    -webkit-appearance: none;
                                    border: 2px solid #3c9639;
                                    border-radius: 50%;
                                    width: 6px; 
                                    height: 6px;
                                    transition: 0.2s all linear;
                                    outline: none;
                                    margin-right: 5px;
                                    position: relative;
                                    top: 4px;
                                    vertical-align: middle;
                                """,
                            },
                            {
                                "selector": "input[type=radio]:checked",
                                "rule": """
                                    border: 2px solid #3c9639;
                                    background-color: #3c9639;  
                                """,
                            },
                            {
                                "selector": ".dash-filter input",  # Add this for filter input
                                "rule": "color: white !important;",
                            },
                            {
                                "selector": ".dash-table-container td:first-child",  # Add this block
                                "rule": "white-space: nowrap !important;",
                            },
                        ],
                        "tooltip_data": [
                            {
                                column: {"value": str(value), "type": "markdown"}
                                for column, value in row.items()
                            }
                            for row in data_quality_df.to_dict("records")
                        ],
                        "tooltip_duration": None,
                    },
                },
            ],
            "interactions": [
                {
                    "triggers": [
                        {
                            "component_id": "data-quality-metrics-table",
                            "component_property": "selected_rows",
                            "input_key": "selected_rows",
                        },
                    ],
                    "outputs": [
                        {
                            "component_id": "dataquality-timeseries-placeholder",
                            "component_property": "children",
                        },
                    ],
                    "action": "update_components_based_on_table_selection",
                    "data_source": {
                        "table_data": data_quality_df,
                        "data_dict": timeseries_data_dict,
                        "include_data_dict_in_download": False,
                    },
                    "index_column": "Stream ID",
                },
            ],
        },
    }

    return plot_config

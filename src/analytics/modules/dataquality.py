"""
This module performs data quality analysis on sensor data.

It includes functions for preprocessing sensor data, analyzing gaps,
detecting outliers, and generating summary statistics and visualizations.
"""

import numpy as np
import pandas as pd

from analytics.dbmgr import DBManager


def _preprocess_to_sensor_rows(db: DBManager):
    sensor_data = []
    all_streams = db.get_all_streams()

    for stream_id, df in all_streams.items():
        try:
            label = db.get_label(stream_id)

            if df.empty:
                continue

            row = {
                "stream_id": stream_id,
                "Label": label,
                "Timestamps": df["time"],
                "Values": df["value"].values,
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
            }
            sensor_data.append(row)
        except Exception:
            continue

    return pd.DataFrame(sensor_data)


def _profile_groups(df):
    """
    Calculate group statistics for sensor data.

    Args:
        df (pd.DataFrame): Preprocessed sensor data.

    Returns:
        pd.DataFrame: Sensor data with added group statistics.
    """
    grouped = df.groupby("Label")

    def calculate_group_stats(group):
        return pd.Series(
            {
                "Group_Mean": group["Sensor_Mean"].mean(),
                "Group_Std": group["Sensor_Mean"].std(),
                "Group_Min": group["Sensor_Min"].min(),
                "Group_Max": group["Sensor_Max"].max(),
            }
        )

    group_stats = grouped.apply(calculate_group_stats)
    return df.merge(group_stats, left_on="Label", right_index=True)


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
    """Prepare the data quality DataFrame with properly named columns."""

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
            "Start Timestamp": df["Start_Timestamp"],
            "End Timestamp": df["End_Timestamp"],
        }
    )

    return data_quality_df


def _create_summary_table(data_quality_df):
    """Create a summary table from data quality metrics."""
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
    summary_table = summary_table.rename(columns={"Stream ID": "Number of Streams"})

    return summary_table


def _get_data_quality_overview(data_quality_df):
    """
    Generate overview analysis of data quality.
    """
    # Create a 'has_outliers' column
    data_quality_df["has_outliers"] = data_quality_df["Outliers"] > 0

    # Pie chart for outliers
    outliers_pie = {
        "title": "Sensors with Outliers",
        "labels": "has_outliers",
        "textinfo": "percent+label",
        "filter": None,
        "dataframe": data_quality_df,
        "kwargs": {
            "marker": {"colors": ["#2d722c", "#3c9639"]}  # Just two colors for binary
        },
    }

    # Pie chart for gaps
    gaps_pie = {
        "title": "Sensors with Gaps",
        "labels": "Brick Class",
        "textinfo": "percent+label",
        "filter": None,
        "dataframe": data_quality_df,
        "kwargs": {
            "marker": {
                "colorscale": [
                    [0, "#1e4c1d"],  # Darkest green
                    [0.5, "#3c9639"],  # Medium green (your theme color)
                    [1, "#b8e4b7"],  # Lightest green
                ]
            }
        },
    }

    # Create stats DataFrame
    stats_df = pd.DataFrame(
        {
            "Metric": [
                "Total Sensors",
                "Sensors with Outliers",
                "Sensors with Gaps",
                "Total Small Gaps",
                "Total Medium Gaps",
                "Total Large Gaps",
                "Average Gap Percentage",
            ],
            "Value": [
                len(data_quality_df),
                data_quality_df["has_outliers"].sum(),
                (data_quality_df["Total Gaps"] > 0).sum(),
                data_quality_df["Small Gaps"].sum(),
                data_quality_df["Medium Gaps"].sum(),
                data_quality_df["Large Gaps"].sum(),
                f"{data_quality_df['Gap Percentage'].mean():.2%}",
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

    # Add histogram configuration
    stream_histogram = {
        "title": "Stream Counts by Label Type",
        "type": "Histogram",  # Match existing plot type pattern
        "x-axis": "Brick Class",
        "y-axis": "Stream_Count",  # Using the column name from summary table
        "x-axis_label": "Label Type",
        "y-axis_label": "Number of Streams",
        "dataframe": data_quality_df.groupby("Brick Class")
        .size()
        .reset_index(name="Stream_Count"),
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

    # Sort the data by start timestamp
    timeline_data = timeline_data.sort_values("Start Timestamp")

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
        "pie_charts": [outliers_pie, gaps_pie],
        "tables": [overall_stats],
        "histogram": stream_histogram,
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
    elif granularity % 3600 == 0:  # 3600 seconds = 1 hour
        return f"{granularity // 3600} hours"
    elif 0 <= granularity % 60 < 5:  # 60 seconds = 1 minute
        return f"{granularity // 60} minutes"
    elif granularity % 60 > 55:
        return f"{1 + granularity // 60} minutes"
    else:
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
    """Build plot components for a stream."""
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
                    "x": 0.5,
                    "xanchor": "center",
                },
                "font_color": "black",
                "plot_bgcolor": "white",
                "autosize": True,
                "legend": {  # Added legend configuration
                    "orientation": "h",
                    "yanchor": "top",
                    "y": -0.3,
                    "xanchor": "center",
                    "x": 0.5,
                    "font": {"size": 12},
                },
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
            "css": {
                "padding": "10px",
            },
        }
    ]


def generate_green_scale(n_colors):
    """Generate a green color scale with the specified number of colors."""
    import numpy as np

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
        hex_color = "#{:02x}{:02x}{:02x}".format(int(rgb[0]), int(rgb[1]), int(rgb[2]))
        colors.append(hex_color)

    return colors


def run(db: DBManager) -> dict:
    """Run all analyses and return the results."""
    df = _preprocess_to_sensor_rows(db)

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
        title = f"{stream_type} Timeseries Data<br><sup>Stream ID: {stream_id}</sup>"

        timeseries_data_dict[stream_id] = _build_components(stream_df, stream_id, title)

    plot_config = {
        ("DataQuality", "Overview"): {
            "title": "Data Quality Overview",
            "components": [
                {
                    "type": "table",
                    "dataframe": overview_data["tables"][0]["dataframe"],
                    "id": "data-quality-overall-stats-table",
                    "title": "Overall Data Quality Statistics",
                    "title_element": "H5",
                    "kwargs": {
                        "columns": [{"name": i, "id": i} for i in ["Metric", "Value"]],
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
                    "data_frame": data_quality_df,
                    "trace_type": "Pie",
                    "data_mappings": {
                        "labels": "has_outliers",
                    },
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
                            "text": "Sensors with Outliers",
                            "font_color": "black",
                            "x": 0.5,
                            "xanchor": "center",
                        },
                    },
                    "css": {
                        "width": "50%",
                        "display": "inline-block",
                        "padding": "5px",
                        "marginTop": "0%",
                        "marginBottom": "5%",
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
                        "marker": {"colors": generate_green_scale(n_classes)},
                    },
                    "layout_kwargs": {
                        "title": {
                            "text": "Outlier Percentage by Class",
                            "font_color": "black",
                            "x": 0.5,
                            "xanchor": "center",
                        },
                    },
                    "css": {
                        "width": "50%",
                        "display": "inline-block",
                        "padding": "10px",
                        "marginTop": "0%",
                        "marginBottom": "5%",
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
                        "xaxis": {
                            "title": "Class Type",
                            "tickangle": 45,  # Slant the labels by 45 degrees
                            "tickfont": {"size": 10},  # Reduce font size
                            "tickmode": "auto",
                            "automargin": True,  # Automatically adjust margins
                        },
                        "yaxis_title": "Number of Streams",
                        "showlegend": False,
                        "bargap": 0.2,
                        "height": 600,
                        "width": 1000,
                        "margin": {"t": 50, "b": 50, "l": 50, "r": 50},
                    },
                    "css": {
                        "width": "60%",
                        "display": "block",
                        "padding": "10px",
                        "marginLeft": "10%",
                        "marginTop": "5%",
                        "marginBottom": "10%",
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
                            "x": 0.6,
                            "xanchor": "center",
                            "font_color": "black",
                            "font_size": 16,
                        },
                        "xaxis": {
                            "title": overview_data["timeline"]["x-axis_label"],
                            "type": "date",
                            "tickformat": "%Y-%m-%d",
                            "tickfont": {"size": 10},
                            "automargin": True,
                            "range": [
                                overview_data["timeline"]["dataframe"][
                                    "Start Timestamp"
                                ].min(),
                                overview_data["timeline"]["dataframe"][
                                    "End Timestamp"
                                ].max(),
                            ],
                        },
                        "yaxis": {"tickfont": {"size": 8}, "automargin": True},
                        "showlegend": False,
                        "height": 600,
                        "width": 1000,
                        "margin": {"t": 50, "b": 50, "l": 200, "r": 50, "pad": 4},
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
                        "width": "100%",
                        "display": "block",
                        "padding": "10px",
                        "marginLeft": "0%",
                        "marginTop": "0%",
                        "marginBottom": "15%",
                    },
                },
            ],
        },
        ("DataQuality", "ByClass"): {
            "title": "Data Quality by Stream Class",
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
                    "type": "table",
                    "dataframe": summary_table_df,
                    "id": "data-quality-by-class-table",
                    "title": "Data Quality Summary by Label",
                    "title_element": "H5",
                    "kwargs": {
                        "columns": [
                            {"name": i, "id": i} for i in summary_table_df.columns
                        ],
                        "row_selectable": "single",
                        "selected_rows": [0],
                        "export_format": "csv",
                        "fixed_rows": {"headers": True},
                        "sort_action": "native",
                        "sort_mode": "multi",
                        "style_header": {
                            "fontWeight": "bold",
                            "backgroundColor": "#3c9639",
                            "color": "white",
                            "textAlign": "center",
                            "whiteSpace": "normal",  # Allow text wrapping in headers
                            "height": "auto",  # Adjust height automatically
                            "minWidth": "100px",  # Minimum width for columns
                            "textAlign": "center",
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
                            "height": 1500,
                            "overflowX": "auto",
                            "minWidth": "100%",  # Table takes full width
                        },
                        "style_data": {"textAlign": "center"},  # Center all cell data
                        "style_data_conditional": [
                            {
                                "if": {"row_index": "odd"},
                                "backgroundColor": "#ddf2dc",
                            }
                        ],
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
                    },
                    "index_column": "Brick Class",
                }
            ],
        },
        ("DataQuality", "ByStream"): {
            "title": "Data Quality Metrics by Stream",
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
                    "type": "table",
                    "dataframe": data_quality_df,
                    "id": "data-quality-metrics-table",
                    "title": "Data Quality Metrics",
                    "title_element": "H5",
                    "kwargs": {
                        "columns": [
                            {"name": col, "id": col}
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
                                "Start Timestamp",
                                "End Timestamp",
                            ]
                        ],
                        "export_format": "csv",
                        "fixed_rows": {"headers": True},
                        "row_selectable": "single",
                        "selected_rows": [0],
                        "sort_action": "native",
                        "sort_mode": "multi",
                        "style_data_conditional": [
                            {
                                "if": {"row_index": "odd"},
                                "backgroundColor": "#ddf2dc",
                            }
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
                            "maxWidth": "180px",  # Maximum width for columns
                            "overflow": "hidden",
                            "textOverflow": "ellipsis",
                        },
                        "style_table": {
                            "height": 1500,
                            "overflowX": "auto",
                            "minWidth": "100%",  # Table takes full width
                        },
                        "style_data": {"textAlign": "center"},  # Center all cell data
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
                        "db": db,
                    },
                    "index_column": "Stream ID",
                },
            ],
        },
    }

    return plot_config


if __name__ == "__main__":

    DATA = "../datasets/bts_site_b_train/train.zip"
    MAPPER = "../datasets/bts_site_b_train/mapper_TrainOnly.csv"
    MODEL = "../datasets/bts_site_b_train/Site_B.ttl"
    SCHEMA = "../datasets/bts_site_b_train/Brick_v1.2.1.ttl"

    import sys

    sys.path.append("..")

    from dbmgr import DBManager

    db = DBManager(DATA, MAPPER, MODEL, SCHEMA)

    results = run(db)
    print(results)

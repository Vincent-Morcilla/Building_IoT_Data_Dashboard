"""
This module performs data quality analysis on sensor data.

It includes functions for preprocessing sensor data, analyzing gaps,
detecting outliers, and generating summary statistics and visualizations.
"""

import os
from typing import Dict, Any

import numpy as np
import pandas as pd
from scipy import stats


def _preprocess_to_sensor_rows(db):
    """
    Preprocess sensor data from the database into a DataFrame.

    Args:
        db: Database object containing sensor data.

    Returns:
        pd.DataFrame: Preprocessed sensor data.
    """
    sensor_data = []
    all_streams = db.get_all_streams()

    for stream_id, df in all_streams.items():
        try:
            label = db.get_label(stream_id)

            if df.empty:
                print(f"Warning: Empty DataFrame for stream {stream_id}")
                continue

            row = {
                "StreamID": stream_id,
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
                "Sensor_Max": df["value"].max(),
                "Sensor_Min": df["value"].min(),
            }
            sensor_data.append(row)
        except Exception as e:
            print(f"Error processing stream {stream_id}: {str(e)}")

    result_df = pd.DataFrame(sensor_data)
    print(f"Preprocessed data shape: {result_df.shape}")
    print(f"Columns: {result_df.columns}")
    print(result_df.head())
    return result_df


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
        return pd.Series({
            "Group_Mean": group["Sensor_Mean"].mean(),
            "Group_Std": group["Sensor_Mean"].std(),
            "Group_Min": group["Sensor_Min"].min(),
            "Group_Max": group["Sensor_Max"].max(),
        })

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
            return pd.Series({
                "Small_Gap_Count": 0,
                "Medium_Gap_Count": 0,
                "Large_Gap_Count": 0,
                "Total_Gaps": 0,
                "Gap_Percentage": 0,
                "Total_Gap_Size_Seconds": 0,
            })

        granularity_parts = str(granularity).split()
        time_granularity = int(granularity_parts[0])
        time_unit = granularity_parts[1]
        granularity_in_seconds = time_granularity * {
            "minutes": 60,
            "hours": 3600
        }.get(time_unit, 86400)

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

        return pd.Series({
            "Small_Gap_Count": small_gap,
            "Medium_Gap_Count": medium_gap,
            "Large_Gap_Count": large_gap,
            "Total_Gaps": total_gaps,
            "Gap_Percentage": gap_percentage,
            "Total_Gap_Size_Seconds": total_gap_size_seconds,
        })

    return df.apply(process_row, axis=1)


def _prepare_data_quality_df(sensor_df):
    """
    Prepare a DataFrame with data quality metrics.

    Args:
        sensor_df (pd.DataFrame): Preprocessed sensor data.

    Returns:
        pd.DataFrame: Data quality metrics.
    """
    columns = [
        "StreamID", "Label", "Deduced_Granularity", "Value_Count",
        "Outliers", "Missing", "Zeros", "Small_Gap_Count",
        "Medium_Gap_Count", "Large_Gap_Count", "Total_Gaps",
        "Gap_Percentage", "Total_Gap_Size_Seconds", "Group_Mean",
        "Group_Std", "Sensor_Mean", "Sensor_Max", "Sensor_Min",
        "Start_Timestamp", "End_Timestamp",
    ]
    data_quality_df = sensor_df[columns].copy()

    data_quality_df["Outlier_Count"] = data_quality_df["Outliers"]
    data_quality_df["Time_Delta"] = (
        data_quality_df["End_Timestamp"] - data_quality_df["Start_Timestamp"]
    )
    data_quality_df["Total_Time_Delta_Seconds"] = (
        data_quality_df["Time_Delta"].dt.total_seconds().round().astype(int)
    )

    # Calculate FlaggedForRemoval based on group statistics
    data_quality_df["FlaggedForRemoval"] = (
        data_quality_df.groupby("Label")
        .apply(
            lambda group: (
                (group["Sensor_Mean"] < (group["Group_Mean"] - 3 * group["Group_Std"]))
                | (group["Sensor_Mean"] > (group["Group_Mean"] + 3 * group["Group_Std"]))
            ).astype(int)
        )
        .reset_index(level=0, drop=True)
    )

    # Skip flagging for groups with std = 0
    data_quality_df.loc[data_quality_df["Group_Std"] == 0, "FlaggedForRemoval"] = 0

    for col in ["Group_Mean", "Group_Std", "Sensor_Mean", "Sensor_Max", "Sensor_Min"]:
        data_quality_df[col] = data_quality_df[col].astype(float)

    return data_quality_df


def _create_summary_table(data_quality_df):
    """
    Create a summary table from data quality metrics.

    Args:
        data_quality_df (pd.DataFrame): Data quality metrics.

    Returns:
        pd.DataFrame: Summary table.
    """
    def sum_timedelta(x):
        if pd.api.types.is_timedelta64_dtype(x):
            return x.sum()
        else:
            return pd.to_timedelta(x).sum()

    summary_table = (
        data_quality_df.groupby("Label")
        .agg({
            "StreamID": "count",
            "Deduced_Granularity": lambda x: stats.mode(x)[0][0],
            "Start_Timestamp": "min",
            "End_Timestamp": "max",
            "Value_Count": "sum",
            "Outlier_Count": "sum",
            "Missing": "sum",
            "Zeros": "sum",
            "Small_Gap_Count": "sum",
            "Medium_Gap_Count": "sum",
            "Large_Gap_Count": "sum",
            "Total_Gaps": "sum",
            "Group_Mean": "first",
            "Group_Std": "first",
            "FlaggedForRemoval": "sum",
            "Time_Delta": sum_timedelta,
            "Total_Gap_Size_Seconds": "sum",
            "Total_Time_Delta_Seconds": "sum",
        })
        .reset_index()
    )

    # Calculate Total_Gap_Percent
    summary_table["Total_Gap_Percent"] = (
        summary_table["Total_Gap_Size_Seconds"]
        / summary_table["Total_Time_Delta_Seconds"]
    ).fillna(0)

    # Format Total_Gap_Percent as percentage string
    summary_table["Total_Gap_Percent"] = summary_table["Total_Gap_Percent"].apply(
        lambda x: f"{x:.2%}"
    )

    # Format Time_Delta as string
    summary_table["Total_Time_Delta"] = summary_table["Time_Delta"].apply(str)

    # Rename columns for clarity
    column_renames = {
        "StreamID": "Stream_Count",
        "Deduced_Granularity": "Common_Granularity",
        "Start_Timestamp": "Earliest_Timestamp",
        "End_Timestamp": "Latest_Timestamp",
        "Value_Count": "Total_Value_Count",
        "Outlier_Count": "Total_Outliers",
        "Missing": "Total_Missing",
        "Zeros": "Total_Zeros",
        "Small_Gap_Count": "Total_Small_Gaps",
        "Medium_Gap_Count": "Total_Medium_Gaps",
        "Large_Gap_Count": "Total_Large_Gaps",
        "FlaggedForRemoval": "Total_Flagged_For_Removal",
        "Time_Delta": "Total_Time_Delta",
    }
    summary_table = summary_table.rename(columns=column_renames)

    # Reorder columns
    column_order = [
        "Label", "Stream_Count", "Common_Granularity", "Earliest_Timestamp",
        "Latest_Timestamp", "Total_Value_Count", "Total_Outliers",
        "Total_Missing", "Total_Zeros", "Group_Mean", "Group_Std",
        "Total_Small_Gaps", "Total_Medium_Gaps", "Total_Large_Gaps",
        "Total_Gaps", "Total_Time_Delta_Seconds", "Total_Gap_Size_Seconds",
        "Total_Gap_Percent", "Total_Flagged_For_Removal",
    ]
    summary_table = summary_table[column_order]

    # Sort the summary table by Total_Gap_Percent in descending order
    return summary_table.sort_values("Total_Gap_Percent", ascending=False)


def _get_data_quality_overview(data_quality_df):
    """
    Generate overview analysis of data quality.

    Args:
        data_quality_df (pd.DataFrame): Data quality metrics.

    Returns:
        dict: Data quality overview with pie charts and tables.
    """
    # Create a 'has_outliers' column
    data_quality_df['has_outliers'] = data_quality_df['Outlier_Count'] > 0

    # Pie chart for outliers
    outliers_pie = {
        "title": "Sensors with Outliers",
        "labels": "has_outliers",
        "values": "count",
        "names": ["No Outliers", "Has Outliers"],
        "dataframe": data_quality_df
    }

    # Pie chart for gaps
    gaps_pie = {
        "title": "Sensors with Gaps",
        "labels": "Total_Gaps",
        "values": "count",
        "names": ["No Gaps", "Has Gaps"],
        "dataframe": data_quality_df
    }

    # Table for overall statistics
    overall_stats = {
        "title": "Overall Data Quality Statistics",
        "dataframe": pd.DataFrame({
            "Metric": ["Total Sensors", "Sensors with Outliers", "Sensors with Gaps", "Average Gap Percentage"],
            "Value": [
                len(data_quality_df),
                data_quality_df['has_outliers'].sum(),
                (data_quality_df['Total_Gaps'] > 0).sum(),
                data_quality_df['Gap_Percentage'].mean()
            ]
        })
    }

    return {
        "pie_charts": [outliers_pie, gaps_pie],
        "tables": [overall_stats]
    }


def _get_summary_analysis(summary_table_df):
    """
    Generate summary analysis.

    Args:
        summary_table_df (pd.DataFrame): Summary table.

    Returns:
        dict: Summary analysis.
    """
    return {
        "Table": {
            "title": "Data Quality Summary by Label",
            "columns": summary_table_df.columns.tolist(),
            "dataframe": summary_table_df,
        }
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

    granularity = pd.Series(time_diffs).mode()[0]  # Most common time interval in seconds

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


def _create_data_quality_plot(data_quality_df):
    """
    Create a plot configuration for data quality metrics.

    Args:
        data_quality_df (pd.DataFrame): Data quality metrics.

    Returns:
        dict: Plot configuration.
    """
    return {
        "type": "table",
        "header": {
            "values": list(data_quality_df.columns),
            "align": "center",
            "line": {"width": 1, "color": "black"},
            "fill": {"color": "grey"},
            "font": {"family": "Arial", "size": 12, "color": "white"},
        },
        "cells": {
            "values": data_quality_df.values.T,
            "align": "center",
            "line": {"color": "black", "width": 1},
            "font": {"family": "Arial", "size": 11, "color": ["black"]},
        },
    }


def _create_summary_plot(summary_table_df):
    """
    Create a plot configuration for the summary table.

    Args:
        summary_table_df (pd.DataFrame): Summary table.

    Returns:
        dict: Plot configuration.
    """
    return {
        "type": "table",
        "header": {
            "values": list(summary_table_df.columns),
            "align": "center",
            "line": {"width": 1, "color": "black"},
            "fill": {"color": "grey"},
            "font": {"family": "Arial", "size": 12, "color": "white"},
        },
        "cells": {
            "values": summary_table_df.values.T,
            "align": "center",
            "line": {"color": "black", "width": 1},
            "font": {"family": "Arial", "size": 11, "color": ["black"]},
        },
    }


def _save_data_quality_to_csv(data_quality_df, output_dir):
    """
    Save the data quality DataFrame to a CSV file.

    Args:
        data_quality_df (pd.DataFrame): Data quality metrics.
        output_dir (str): Output directory path.
    """
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    output_file_path = os.path.join(output_dir, "data_quality_table.csv")
    data_quality_df.to_csv(output_file_path, index=False)
    print(f"Data quality table saved to {output_file_path}")


def _save_summary_table_to_csv(summary_table_df, output_dir):
    """
    Save the summary table DataFrame to a CSV file.

    Args:
        summary_table_df (pd.DataFrame): Summary table.
        output_dir (str): Output directory path.
    """
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    output_file_path = os.path.join(output_dir, "summary_table.csv")
    summary_table_df.to_csv(output_file_path, index=False)
    print(f"Summary table saved to {output_file_path}")

def run(db: Any) -> Dict[str, Any]:
    """
    Run all analyses and return the results.

    Args:
        db (Any): Database object.

    Returns:
        Dict[str, Any]: Analysis results.
    """
    print("Running Data Quality Analysis...")
    df = _preprocess_to_sensor_rows(db)
    df = _profile_groups(df)
    gap_analysis_results = _analyse_sensor_gaps(df)
    df = pd.concat([df, gap_analysis_results], axis=1)

    data_quality_df = _prepare_data_quality_df(df)
    summary_table_df = _create_summary_table(data_quality_df)

    output_dir = os.path.join(os.getcwd(), "output")

    # Save data quality table to CSV
    _save_data_quality_to_csv(data_quality_df, output_dir)

    # Save summary table to CSV
    _save_summary_table_to_csv(summary_table_df, output_dir)

    overview_data = _get_data_quality_overview(data_quality_df)

    return {
        "DataQuality_DataQualityTable": {
            "Table": {
                "title": "Data Quality Metrics",
                "dataframe": data_quality_df,
                "columns": list(data_quality_df.columns),
            }
        },
        "DataQuality_SummaryTable": {
            "Table": {
                "title": "Data Quality Summary",
                "dataframe": summary_table_df,
                "columns": list(summary_table_df.columns),
            }
        },
        "DataQuality_Overview": {
            "PieChartAndTable": {
                "title": "Data Quality Overview",
                "pie_charts": overview_data["pie_charts"],
                "tables": overview_data["tables"],
            }
        },
        "DataQuality_SummaryAnalysis": {
            "Table": {
                "title": "Data Quality Summary by Label",
                "dataframe": _get_summary_analysis(summary_table_df),
                "columns": ["Label", "Quality Score", "Interpretation"],
            }
        },
    }


if __name__ == "__main__":
    from dbmgr import Database

    db = Database() 
    results = run(db)
    print(results)


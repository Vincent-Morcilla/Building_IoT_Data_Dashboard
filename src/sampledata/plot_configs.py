"""
Defines sample configurations for plots and interactive components.

This module provides a set of predefined configurations for various types of
plots, tables, and UI components used in the application. Each configuration
specifies the layout, styling, data mappings, and interactivity for a given
plot or component, organized by category and subcategory. 

These sample configurations serve as examples for defining and structuring
visualisations, including timeseries plots, histograms, heatmaps, and
interactive tables. They also illustrate how to specify component properties,
data sources, and actions, facilitating the creation of complex, interactive
visuals within the app.
"""

import string

import numpy as np
import pandas as pd

# Set seed for reproducibility
np.random.seed(42)

# Create a DataFrame for the DataTable
table_data_frame = pd.DataFrame(
    {
        "Stream_IDs": ["streamID1", "streamID2", "streamID3"],
        "Description": ["First stream", "Second stream", "Third stream"],
        "Some_Metric_1": [42, 37, 58],
        "Some_Metric_2": [52, 47, 38],
        "Some_Metric_3": [22, 17, 88],
        "Some_Metric_4": [12, 27, 38],
        "Some_Metric_5": [32, 67, 48],
        "Some_Metric_6": [62, 57, 18],
        "Some_Metric_7": [72, 77, 78],
        "Some_Metric_8": [82, 87, 68],
    }
)

# Create a timeseries_data_dict with components
timeseries_data_dict = {}

for stream_id in table_data_frame["Stream_IDs"]:
    # Create sample data for plotting
    df_plot = pd.DataFrame(
        {
            "Timestamp": pd.date_range(start="2023-01-01", periods=100, freq="D"),
            "Value": np.random.randn(100).cumsum(),
        }
    )

    # Create sample data for table
    df_table = pd.DataFrame(
        {
            "Metric": np.random.choice(list(string.ascii_uppercase), 3),
            "Value": np.random.rand(3),
        }
    )

    # Components for this streamID
    components = [
        {
            "type": "plot",
            "library": "px",
            "function": "line",
            "id": f"line-plot-{stream_id}",
            "kwargs": {
                "data_frame": df_plot,
                "x": "Timestamp",
                "y": "Value",
                "title": f"Timeseries Plot for {stream_id}",
            },
            "layout_kwargs": {
                "title": {
                    "text": f"Timeseries Plot for {stream_id}",
                    "x": 0.5,
                    "xanchor": "center",
                },
                "font_color": "black",
                "plot_bgcolor": "white",
                "autosize": True,
                "legend": {
                    "orientation": "h",
                    "yanchor": "top",
                    "y": -0.2,
                    "xanchor": "center",
                    "x": 0.5,
                    "font": {"size": 15},
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
        },
        # Separator
        {
            "type": "separator",
            "style": {"margin": "20px 0"},
        },
        # Table component
        {
            "type": "table",
            "dataframe": df_table,
            "id": f"table-{stream_id}",
            "title": f"Metrics for {stream_id}",
            "title_element": "H4",
            "kwargs": {
                "columns": [{"name": col, "id": col} for col in df_table.columns],
                "page_size": 5,
                "style_cell": {
                    "fontSize": 14,
                    "textAlign": "left",
                    "padding": "5px",
                    "maxWidth": 0,
                },
                "style_header": {
                    "fontWeight": "bold",
                    "backgroundColor": "#3c9639",
                    "color": "white",
                },
                "style_data_conditional": [
                    {
                        "if": {"row_index": "odd"},
                        "backgroundColor": "#ddf2dc",
                    }
                ],
                "style_table": {
                    "overflowX": "auto",
                },
                "export_format": "csv",
            },
            "css": {
                "padding": "10px",
            },
        },
    ]
    # Assign the components to the streamID in the dict
    timeseries_data_dict[stream_id] = components


# Mock Data for Timeline Plot
timeline_data = pd.DataFrame(
    {
        "Sensor": ["Sensor A", "Sensor B", "Sensor C"],
        "Start_Timestamp": pd.to_datetime(["2023-01-01", "2023-02-15", "2023-03-10"]),
        "End_Timestamp": pd.to_datetime(["2023-04-01", "2023-05-20", "2023-06-30"]),
        "Num_Streams": [5, 3, 8],
    }
)

timeline_data["Duration"] = (
    timeline_data["End_Timestamp"] - timeline_data["Start_Timestamp"]
).dt.total_seconds() * 1000
timeline_data["Hover_Text"] = timeline_data.apply(
    lambda row: (
        f"<b>{row['Sensor']}</b><br>"
        f"Start: {row['Start_Timestamp'].strftime('%Y-%m-%d')}<br>"
        f"End: {row['End_Timestamp'].strftime('%Y-%m-%d')}<br>"
        f"Number of Streams: {row['Num_Streams']}<br>"
        "<extra></extra>"
    ),
    axis=1,
)


# Plot Configurations
sample_plot_configs = {
    # Sunburst Chart for Building Structure
    ("BuildingStructure", "BuildingStructure"): {
        "title": None,
        "components": [
            {
                "type": "plot",
                "library": "px",
                "function": "sunburst",
                "id": "building-structure-sunburst",
                "kwargs": {
                    "data_frame": pd.DataFrame(
                        {
                            "BuildingID": [
                                "Building A",
                                "Building A",
                                "Building B",
                                "Building B",
                            ],
                            "ParentID": [
                                "Building A",
                                "Room 1",
                                "Building B",
                                "Room 2",
                            ],
                            "EntityType": ["Room", "Device", "Room", "Device"],
                            "EntityID": ["Room 1", "Device 1", "Room 2", "Device 2"],
                        }
                    ),
                    "path": ["BuildingID", "ParentID", "EntityID"],
                    "title": "Building Structure",
                    "height": 1000,
                    "width": 1000,
                    "color": "BuildingID",
                    "color_discrete_map": {
                        "Building A": "lightgreen",
                        "Building B": "White",
                    },
                },
                "layout_kwargs": {
                    "title": {
                        "text": "Building Structure",
                        "x": 0.5,
                        "xanchor": "center",
                        "font": {"size": 35},
                    },
                    "font_color": "black",
                    "plot_bgcolor": "white",
                    "coloraxis_colorbar": {
                        "title": "Level",
                        "orientation": "h",
                        "yanchor": "top",
                        "y": -0.2,
                        "xanchor": "center",
                        "x": 0.5,
                    },
                },
                "css": {
                    "padding": "10px",
                },
            },
        ],
    },
    # Box and Whisker Plot for Data Quality
    ("Consumption", "ConsumptionDataQuality"): {
        "title": None,
        "components": [
            {
                "type": "plot",
                "library": "px",
                "function": "box",
                "id": "consumption-quality-box-plot",
                "kwargs": {
                    "data_frame": pd.DataFrame(
                        {
                            "Measurement": ["Quality A"] * 50
                            + ["Quality B"] * 50
                            + ["Quality C"] * 50,
                            "Value": np.random.normal(loc=60, scale=15, size=150),
                        }
                    ),
                    "x": "Measurement",
                    "y": "Value",
                    "color": "Measurement",
                },
                "layout_kwargs": {
                    "title": {
                        "text": "Consumption Data Quality Analysis",
                        "x": 0.5,
                        "xanchor": "center",
                    },
                    "font_color": "black",
                    "plot_bgcolor": "white",
                    "margin": {"l": 50, "r": 50, "b": 100, "t": 50},
                    "autosize": True,
                    "font": {"size": 15},
                    "legend": {
                        "orientation": "h",
                        "yanchor": "top",
                        "y": -0.2,
                        "xanchor": "center",
                        "x": 0.5,
                        "font": {"size": 15},
                    },
                    "xaxis": {
                        "title": None,
                        "mirror": True,
                        "ticks": "outside",
                        "showline": True,
                        "linecolor": "black",
                        "gridcolor": "lightgrey",
                    },
                    "yaxis": {
                        "title": "Quality Value",
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
            },
            # Separator
            {"type": "separator", "style": {"margin": "20px 0"}},
            # UI Component: Dropdown for Measurements
            {
                "type": "UI",
                "element": "Dropdown",
                "id": "consumption-quality-dropdown",
                "label": "Select Variables:",
                "label_position": "above",  # or "next"
                "kwargs": {
                    "options": [
                        {"label": measurement, "value": measurement}
                        for measurement in ["Quality A", "Quality B", "Quality C"]
                    ],
                    "value": ["Quality A", "Quality B", "Quality C"],
                    "multi": True,
                    "clearable": False,
                },
                "css": {
                    "margin": "0 auto",
                    "padding": "10px",
                },
            },
        ],
        "interactions": [
            {
                "triggers": [
                    {
                        "component_id": "consumption-quality-dropdown",
                        "component_property": "value",
                        "input_key": "selected_measurements",  # Placeholder for inputs
                    },
                ],
                "outputs": [
                    {
                        "component_id": "consumption-quality-box-plot",
                        "component_property": "figure",
                    },
                ],
                "action": "process_interaction",
                "data_mapping": {
                    "from": "consumption-quality-box-plot",
                    "to": "consumption-quality-box-plot",
                },
                "data_processing": {
                    "filter": {
                        "Measurement": {
                            "in": "selected_measurements",
                        },
                    },
                },
            },
        ],
    },
    # Line Plot for Consumption General Analysis
    ("Consumption", "GeneralAnalysis"): {
        "title": None,
        "components": [
            {
                "type": "plot",
                "library": "px",
                "function": "line",
                "id": "consumption-line-plot",
                "kwargs": {
                    "data_frame": pd.DataFrame(
                        {
                            "Timestamp": pd.date_range(
                                start="2023-01-01", periods=365, freq="D"
                            ),
                            "Usage": np.cumsum(
                                np.random.normal(loc=500, scale=50, size=365)
                            ),
                            "Temperature": np.random.normal(loc=20, scale=5, size=365),
                            "Pressure": np.random.normal(loc=1, scale=0.1, size=365),
                        }
                    ),
                    "x": "Timestamp",
                    "y": "Usage",
                },
                "layout_kwargs": {
                    "title": {
                        "text": "Water Usage Over Time",
                        "x": 0.5,
                        "xanchor": "center",
                    },
                    "font_color": "black",
                    "plot_bgcolor": "white",
                    "font": {"size": 15},
                    "legend": {
                        "orientation": "h",
                        "yanchor": "top",
                        "y": -0.3,
                        "xanchor": "center",
                        "x": 0.5,
                        "font": {"size": 15},
                    },
                    "xaxis": {
                        "title": "Date",
                        "mirror": True,
                        "ticks": "outside",
                        "showline": True,
                        "linecolor": "black",
                        "gridcolor": "lightgrey",
                    },
                    "yaxis": {
                        "title": "Usage",
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
            },
            # Separator
            {"type": "separator", "style": {"margin": "20px 0"}},
            # UI Component: Date Range Picker
            {
                "type": "UI",
                "element": "DatePickerRange",
                "id": "consumption-date-picker",
                "label": "Pick a Date Range:",
                "label_position": "next",  # or "above"
                "kwargs": {
                    "min_date_allowed": pd.to_datetime("2023-01-01"),
                    "max_date_allowed": pd.to_datetime("2023-12-31"),
                    "start_date": pd.to_datetime("2023-01-01"),
                    "end_date": pd.to_datetime("2023-12-31"),
                },
                "css": {
                    "width": "80%",
                    "margin": "0 auto",
                    "padding": "10px",
                },
            },
        ],
        "interactions": [
            {
                "triggers": [
                    {
                        "component_id": "consumption-date-picker",
                        "component_property": "start_date",
                        "input_key": "start_date",
                    },
                    {
                        "component_id": "consumption-date-picker",
                        "component_property": "end_date",
                        "input_key": "end_date",
                    },
                ],
                "outputs": [
                    {
                        "component_id": "consumption-line-plot",
                        "component_property": "figure",
                    },
                ],
                "action": "process_interaction",
                "data_mapping": {
                    "from": "consumption-line-plot",
                    "to": "consumption-line-plot",
                },
                "data_processing": {
                    "filter": {
                        "Timestamp": {
                            "between": {
                                "start_date": "start_date",
                                "end_date": "end_date",
                            }
                        },
                    },
                },
            },
        ],
    },
    # Heatmap for Consumption Usage Analysis
    ("Consumption", "UsageAnalysis"): {
        "title": None,
        "components": [
            {
                "type": "plot",
                "library": "go",
                "function": "Heatmap",
                "id": "consumption-heatmap",
                "data_frame": pd.DataFrame(
                    {
                        "Date": pd.date_range(
                            start="2021-01-01", periods=12, freq="MS"
                        ).strftime("%b"),
                        "Sensor": ["Sensor A"] * 6 + ["Sensor B"] * 6,
                        "Correlation": [
                            -0.5,
                            0,
                            0.9,
                            0.8,
                            0.3,
                            -0.4,
                            -0.8,
                            0.5,
                            -0.2,
                            -0.1,
                            0.95,
                            0.13,
                        ],
                    }
                ),
                "trace_type": "Heatmap",  # Specify the trace type
                "data_mappings": {
                    "x": "Date",
                    "y": "Sensor",
                    "z": "Correlation",
                },
                "kwargs": {
                    "colorscale": "Viridis",
                    "colorbar": {
                        "title": "Usage",
                        "orientation": "h",
                        "yanchor": "bottom",
                        "y": -0.7,
                        "xanchor": "center",
                        "x": 0.5,
                        "title_side": "bottom",
                    },
                },
                "layout_kwargs": {
                    "title": {
                        "text": "Water Usage Heatmap",
                        "x": 0.5,
                        "xanchor": "center",
                    },
                    "xaxis_title": "Date",
                    "yaxis_title": "Sensor",
                    "font_color": "black",
                    "plot_bgcolor": "white",
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
            },
        ],
    },
    # Example of a Failed Analysis returning some error message to the user
    ("Failed Analysis", "Error"): {
        "components": [
            {
                "type": "error",
                "message": "I couldn't produce my analysis, here's where I would tell the user why.",
                "css": {"color": "#a93932", "font-weight": "bold"},
            }
        ],
    },
    # Pie Charts and Tables for Model Quality - Class Consistency
    ("ModelQuality", "ClassConsistency"): {
        "title": "Class Consistency Analysis",
        "components": [
            # Pie Chart: Consistent vs Inconsistent Classes
            {
                "type": "plot",
                "library": "px",
                "function": "pie",
                "id": "class-consistency-pie",
                "kwargs": {
                    "data_frame": pd.DataFrame(
                        {
                            "entity": ["Entity_1", "Entity_2", "Entity_3", "Entity_4"],
                            "brick_class": ["Class_A", "Class_B", "Class_C", "Class_D"],
                            "brick_class_in_mapping": [
                                "Class_A",
                                "Class_E",
                                "Class_C",
                                "Class_F",
                            ],
                            "brick_class_is_consistent": [True, False, True, False],
                        }
                    ),
                    "names": "brick_class_is_consistent",
                    "title": "Class Consistency",
                },
                "layout_kwargs": {
                    "title": {
                        "text": "Proportion of Consistent vs Inconsistent Classes",
                        "font_color": "black",
                        "x": 0.5,
                        "xanchor": "center",
                    },
                    "legend": {"font": {"color": "black"}},
                },
                "css": {
                    "width": "45%",
                    "display": "inline-block",
                    "padding": "10px",
                },
            },
            # Pie Chart: Inconsistent Classes by Brick Class
            {
                "type": "plot",
                "library": "go",
                "function": "Figure",
                "id": "inconsistent-classes-pie",
                "data_frame": pd.DataFrame(
                    {
                        "entity": [
                            "Entity_3",
                            "Entity_4",
                            "Entity_3",
                            "Entity_4",
                            "Entity_5",
                        ],
                        "brick_class": [
                            "Class_B",
                            "Class_B",
                            "Class_C",
                            "Class_D",
                            "Class_A",
                        ],
                        "brick_class_in_mapping": [
                            "Class_B",
                            "Class_D",
                            "Class_C",
                            "Class_F",
                            "Class_F",
                        ],
                        "brick_class_is_consistent": [True, False, True, False, False],
                    }
                ),
                "trace_type": "Pie",
                "data_mappings": {
                    "labels": "brick_class",
                    "values": "count",
                },
                "data_processing": {
                    "filter": {"brick_class_is_consistent": False},
                    "groupby": ["brick_class"],
                    "aggregations": {"count": ("brick_class", "count")},
                },
                "kwargs": {
                    "textinfo": "percent+label",
                    "textposition": "inside",
                    "showlegend": False,
                },
                "layout_kwargs": {
                    "title": {
                        "text": "Inconsistent Classes by Brick Class",
                        "font_color": "black",
                        "x": 0.5,
                        "xanchor": "center",
                    },
                },
                "css": {
                    "width": "45%",
                    "display": "inline-block",
                    "padding": "10px",
                },
            },
            # Separator
            {"type": "separator", "style": {"margin": "20px 0"}},
            # Table: Details of Inconsistent Classes
            {
                "type": "table",
                "dataframe": pd.DataFrame(
                    {
                        "entity": ["Entity_2", "Entity_4"],
                        "brick_class": ["Class_B", "Class_D"],
                        "brick_class_in_mapping": ["Class_E", "Class_F"],
                        "brick_class_is_consistent": [False, False],
                    }
                ),
                "id": "inconsistent-classes-table",
                "title": "Details of Inconsistent Classes",
                "title_element": "H2",  # Adjust the HTML heading level as needed
                "title_kwargs": {
                    "style": {
                        "color": "#3c9639",
                    },
                },
                "kwargs": {
                    "columns": [
                        {"name": "Entity", "id": "entity"},
                        {"name": "Brick Class in Model", "id": "brick_class"},
                        {
                            "name": "Brick Class in Mapping",
                            "id": "brick_class_in_mapping",
                        },
                    ],
                    "page_size": 10,
                    "style_cell": {
                        "fontSize": 14,
                        "textAlign": "left",
                        "padding": "5px",
                        "maxWidth": 0,
                    },
                    "style_header": {
                        "fontWeight": "bold",
                        "backgroundColor": "#3c9639",
                        "color": "white",
                    },
                    "style_data_conditional": [
                        {
                            "if": {"row_index": "odd"},
                            "backgroundColor": "#ddf2dc",
                        }
                    ],
                    "style_table": {
                        "overflowX": "auto",
                    },
                    "export_format": "csv",
                },
                "css": {
                    "padding": "10px",
                },
            },
        ],
    },
    ("RoomClimate", "RoomClimate"): {
        "title": "Room Climate Rooms Data",
        "components": [
            # Placeholder Div for dynamic components
            {
                "type": "placeholder",
                "id": "dynamic-components-placeholder",
            },
            # Separator
            {
                "type": "separator",
                "style": {"margin": "20px 0"},
            },
            # UI Component: DataTable
            {
                "type": "UI",
                "element": "DataTable",
                "id": "datatable",
                "label": None,
                "title": "DataTable",  # UI's can now have the same kind of titles as Tables
                "title_element": "H4",
                "kwargs": {
                    "columns": [
                        {
                            "name": " ".join(col.split("_")),
                            "id": col,
                            "type": "numeric" if "Metric" in col else "text",
                        }
                        for col in table_data_frame.columns
                    ],
                    "data": table_data_frame.to_dict("records"),
                    "filter_action": "native",
                    "fixed_columns": {
                        "headers": True,
                        "data": 1,
                    },  # Freeze first column
                    "row_selectable": "single",
                    "selected_rows": [0],  # Default to first row
                    "sort_action": "native",
                    "style_cell": {
                        "fontSize": 14,
                        "textAlign": "left",
                        "padding": "5px",
                        "minWidth": "150px",
                    },
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
                    },
                    "style_table": {
                        "overflowX": "auto",
                        "width": "100%",
                        "minWidth": "100%",
                    },
                },
                "css": {
                    "padding": "5px",  # Changed the padding to match other table
                    "width": "100%",
                },
            },
        ],
        "interactions": [
            {
                "triggers": [
                    {
                        "component_id": "datatable",
                        "component_property": "selected_rows",
                        "input_key": "selected_rows",
                    },
                ],
                "outputs": [
                    {
                        "component_id": "dynamic-components-placeholder",
                        "component_property": "children",
                    },
                ],
                "action": "update_components_based_on_table_selection",
                "data_source": {
                    "table_data": table_data_frame,
                    "data_dict": timeseries_data_dict,  # Pass the dictionary of components
                    "include_data_dict_in_download": True,  # Download all dataframes in data_dict
                },
                "index_column": "Stream_IDs",  # The column used as index as it's known in the dataframe not as it's known in the datatable
            },
        ],
    },
}

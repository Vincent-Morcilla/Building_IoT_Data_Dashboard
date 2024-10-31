import numpy as np
import pandas as pd
import plotly.express as px

# Set seed for reproducibility
np.random.seed(42)

# Sample Data for the Table
table_data = pd.DataFrame({
    'ID': [1, 2, 3],
    'Name': ['Room A', 'Room B', 'Room C'],
    'Description': ['First floor', 'Second floor', 'Third floor'],
})

# Sample Timeseries Data for Each ID
timeseries_data_dict = {}

for id_val in table_data['ID']:
    df = pd.DataFrame({
        'Date': pd.date_range(start='2023-01-01', periods=100),
        'Value': np.random.rand(100).cumsum(),
        'Variable1': np.random.rand(100).cumsum(),
        'Variable2': np.random.rand(100).cumsum(),
    })
    timeseries_data_dict[id_val] = df  # Store each DataFrame with its ID as the key

# Plot Configurations
plot_configs = {
    # Box and Whisker Plot for Data Quality
    ("DataQuality", "ConsumptionDataQuality"): {
        "title": None,
        "components": [
            {
                "type": "plot",
                "library": "px",
                "function": "box",
                "id": "consumption-quality-box-plot",
                "kwargs": {
                    "data_frame": pd.DataFrame({
                        "Measurement": ["Quality A"] * 50 + ["Quality B"] * 50 + ["Quality C"] * 50,
                        "Value": np.random.normal(loc=60, scale=15, size=150),
                    }),
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
            {
                "type": "separator",
                "style": {"margin": "20px 0"}
            },
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
                    "data_frame": pd.DataFrame({
                        "Timestamp": pd.date_range(start="2023-01-01", periods=365, freq="D"),
                        "Usage": np.cumsum(np.random.normal(loc=500, scale=50, size=365)),
                        "Temperature": np.random.normal(loc=20, scale=5, size=365),
                        "Pressure": np.random.normal(loc=1, scale=0.1, size=365),
                    }),
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
                        "font": {"size": 15}
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
            {
                "type": "separator",
                "style": {"margin": "20px 0"}
            },
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
                "action": "update_plot_property",
                "data_source": "consumption-line-plot",
                "update_kwargs": {
                    "data_frame": "data_frame", # Placeholder
                },
                "filters": {
                    "Timestamp": {
                        "between": {
                            "start_date": "start_date",
                            "end_date": "end_date",
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
                "data_frame": pd.DataFrame({
                        "Date": pd.date_range(start="2021-01-01", periods=12, freq="MS").strftime('%b'),
                        "Sensor": ["Sensor A"] * 6 + ["Sensor B"] * 6,
                        "Correlation": [-0.5, 0, 0.9, 0.8, 0.3, -0.4, -0.8, 0.5, -0.2, -0.1, 0.95, 0.13],
                    }),
                "trace_type": "Heatmap",  # Specify the trace type
                "kwargs": {
                    "x": "Date",
                    "y": "Sensor",
                    "z": "Correlation",
                    "colorscale": "Viridis",
                },
                "layout_kwargs": {
                    "title": {"text": "Water Usage Heatmap", "x": 0.5, "xanchor": "center"},
                    "xaxis_title": "Date",
                    "yaxis_title": "Sensor",
                    "font_color": "black",
                    "plot_bgcolor": "white",
                    "coloraxis_colorbar": {
                        "title": "Usage",
                        "orientation": "h",
                        "yanchor": "top",
                        "y": -0.4,
                        "xanchor": "center",
                        "x": 0.5,
                        "title_side": "bottom",
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
                "style": {"margin": "20px 0"}
            },
            # UI Component: Color Scale Dropdown
            {
                "type": "UI",
                "element": "Dropdown",
                "id": "heatmap-color-scale-dropdown",
                "label": "Select Color Scale",
                "label_position": "above",
                "kwargs": {
                    "options": [
                        {"label": scale, "value": scale}
                        for scale in px.colors.named_colorscales()
                    ],
                    "value": "Viridis",
                    "clearable": False,
                },
                "css": {
                    "padding": "10px",
                },
            },
        ],
        "interactions": [
            {
                "triggers": [
                    {
                        "component_id": "heatmap-color-scale-dropdown",
                        "component_property": "value",
                        "input_key": "selected_color_scale",
                    },
                ],
                "outputs": [
                    {
                        "component_id": "consumption-heatmap",
                        "component_property": "figure",
                    },
                ],
                "action": "update_plot_property",
                "data_source": "consumption-heatmap",
                "update_kwargs": {
                    "colorscale": "selected_color_scale",
                },
            },
        ],
    },

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
                    "data_frame": pd.DataFrame({
                        "BuildingID": ["Building A", "Building A", "Building B", "Building B"],
                        "ParentID": ["Building A", "Room 1", "Building B", "Room 2"],
                        "EntityType": ["Room", "Device", "Room", "Device"],
                        "EntityID": ["Room 1", "Device 1", "Room 2", "Device 2"]
                    }),
                    "path": ["BuildingID", "ParentID", "EntityID"],
                    "color": "EntityType",
                    "title": "Building Structure",
                    "height": 1000,
                    "width": 1000,
                },
                "layout_kwargs": {
                    "title": {"text": "Building Structure", "x": 0.5, "xanchor": "center", "font": {"size": 35}},
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

    # Pie Charts and Tables for Model Quality - Class Inconsistency
    ("ModelQuality", "ClassInconsistency"): {
        "title": "Class Inconsistency Analysis",
        "components": [
            # Pie Chart: Consistent vs Inconsistent Classes
            {
                "type": "plot",
                "library": "px",
                "function": "pie",
                "id": "class-consistency-pie",
                "kwargs": {
                    "data_frame": pd.DataFrame({
                        "entity": ["Entity_1", "Entity_2", "Entity_3", "Entity_4"],
                        "brick_class": ["Class_A", "Class_B", "Class_C", "Class_D"],
                        "brick_class_in_mapping": ["Class_A", "Class_E", "Class_C", "Class_F"],
                        "brick_class_is_consistent": [True, False, True, False],
                    }),
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
                    "legend": {
                        "font": {
                            "color": "black"
                        }
                    },
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
                "data_frame": pd.DataFrame({
                        "entity": ["Entity_3", "Entity_4", "Entity_3", "Entity_4", "Entity_5"],
                        "brick_class": ["Class_B", "Class_B", "Class_C", "Class_D", "Class_A"],
                        "brick_class_in_mapping": ["Class_B", "Class_D", "Class_C", "Class_F", "Class_F"],
                        "brick_class_is_consistent": [True, False, True, False, False],
                    }),
                "trace_type": "Pie",
                "data_processing": {
                    "filter": {"brick_class_is_consistent": False},
                    "groupby": ["brick_class"],
                    "aggregation": {"count": ("brick_class", "count")},
                    "data_mappings": {
                        "labels": "brick_class",
                        "values": "count",
                    },
                    "trace_kwargs": {
                        "textinfo": "percent+label",
                        "textposition": "inside",
                        "showlegend": False,
                    },
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
            {
                "type": "separator",
                "style": {"margin": "20px 0"}
            },
            # Table: Details of Inconsistent Classes
            {
                "type": "table",
                "dataframe": pd.DataFrame({
                    "entity": ["Entity_2", "Entity_4"],
                    "brick_class": ["Class_B", "Class_D"],
                    "brick_class_in_mapping": ["Class_E", "Class_F"],
                    "brick_class_is_consistent": [False, False],
                }),
                "id": "inconsistent-classes-table",
                "kwargs": {
                    "columns": [
                        {"name": "Entity", "id": "entity"},
                        {"name": "Brick Class in Model", "id": "brick_class"},
                        {"name": "Brick Class in Mapping", "id": "brick_class_in_mapping"},
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
                    "export_format":"csv",
                },
                "css": {
                    "padding": "10px",
                },
            },
        ],
    },

    ("RoomClimate", "Rooms"): {
        "title": None,
        "components": [
            {
                "type": "plot",
                "library": "px",
                "function": "line",
                "id": "updateable-line-chart",
                "kwargs": {
                    "data_frame": timeseries_data_dict[1],  # Default to ID 1
                    "x": "Date",
                    "y": "Value",
                    "title": "Timeseries for Selected Room",
                },
                "layout_kwargs": {
                    "font_color": "black",
                    "plot_bgcolor": "white",
                    "title": {
                        "text": "Timeseries for Selected Room",
                        "x": 0.5,
                        "xanchor": "center",
                    },
                    "legend": {
                        "orientation": "h",
                        "yanchor": "bottom",
                        "y": -0.3,
                        "xanchor": "center",
                        "x": 0.5,
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
            # UI Component: DataTable
            {
                "type": "UI",
                "element": "DataTable",
                "id": "datatable",
                "label": None,
                "kwargs": {
                    "data": table_data.to_dict('records'),
                    "columns": [
                        {"name": col, "id": col} for col in table_data.columns
                    ],
                    "row_selectable": "single",
                    "selected_rows": [0],  # Default to first row
                    "sort_action": "native",
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
                        "component_id": "datatable",
                        "component_property": "selected_rows",
                        "input_key": "selected_rows",
                    },
                ],
                "outputs": [
                    {
                        "component_id": "updateable-line-chart",
                        "component_property": "figure",
                    },
                ],
                "action": "update_plot_based_on_table_selection",
                "data_source": {
                    "table_data": table_data,
                    "timeseries_data_dict": timeseries_data_dict,  # Pass the dictionary of DataFrames
                },
            },
        ],
    },
}

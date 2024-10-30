import argparse
from collections import defaultdict
import os
import re
import sys

import dash
import dash_bootstrap_components as dbc
from dash import dash_table, Input, Output, dcc, html, State, MATCH, ctx
import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

# FIXME: Remove this hackathon
# Add my code snippets directory to the system path
# sys.path.append(
#     os.path.abspath(os.path.join(os.path.dirname(__file__), "../../code_snippets/tim"))
# )
# import analytics.modelquality as mq
import dbmgr
import analyticsmgr

# m = mq.ModelQuality()

################################################################################
#                               GLOBAL VARIABLES                               #
################################################################################

am = None  # Analytics Manager instance

APP_NAME = "GreenInsight"
# Initialise the Dash app with Bootstrap for styling and allow callbacks for dynamic content
app = dash.Dash(
    # __name__,
    APP_NAME,
    external_stylesheets=[dbc.themes.BOOTSTRAP],
    suppress_callback_exceptions=True,  # Allows callbacks to reference components not yet in the layout
)
app.title = APP_NAME
url_to_key_mapping = {}  # Mapping for dataframes and plot configurations based on URLs
bottom_buttons = None  # Buttons for downloading reports and data

################################################################################
#                                 SAMPLE DATA                                  #
################################################################################

# Sample dataframes representing different datasets used in the app
dataframes = {
    "BuildingStructure_BuildingStructure": pd.DataFrame(
        {
            "BuildingID": ["Building A", "Building A", "Building B", "Building B"],
            "ParentID": ["Building A", "Room 1", "Building B", "Room 2"],
            "EntityType": ["Room", "Device", "Room", "Device"],
            "EntityID": ["Room 1", "Device 1", "Room 2", "Device 2"],
            "Level": [1, 2, 1, 2],
        }
    ),
    "BuildingStructure_DataQuality": pd.DataFrame(
        {
            "Measurement": ["Quality A"] * 50 + ["Quality B"] * 50 + ["Quality C"] * 50,
            "Value": np.random.normal(loc=50, scale=10, size=150),
        }
    ),
    "ModelQuality_ClassInconsistency": pd.DataFrame(
        {
            "entity": ["Entity_1", "Entity_2", "Entity_3", "Entity_4"],
            "brick_class": ["Class_A", "Class_B", "Class_C", "Class_D"],
            "brick_class_in_mapping": ["Class_A", "Class_E", "Class_C", "Class_F"],
            "brick_class_is_consistent": [True, False, True, False],
        }
    ),
    "ModelQuality_MissingTimeseries": pd.DataFrame(
        {
            "stream_id": ["Stream_1", "Stream_2", "Stream_3", "Stream_4"],
            "brick_class": ["Class_A", "Class_B", "Class_C", "Class_D"],
            "has_data": [True, False, True, False],
            "stream_exists_in_mapping": [True, False, True, False],
        }
    ),
    "ModelQuality_RecognisedEntities": pd.DataFrame(
        {
            "entity_id": ["Entity_1", "Entity_2", "Entity_3", "Entity_4", "Entity_5"],
            "brick_class": ["Class_A", "Class_B", "Class_C", "Class_D", "Class_E"],
            "class_in_provided_brick": [True, False, True, False, True],
        }
    ),
    "RoomClimate_WeatherSensitivity": pd.DataFrame(
        {
            "Day": pd.date_range(start="2021-01-01", periods=365, freq="D"),
            "Hour": [i % 24 for i in range(365)],
            "Temperature": [i % 30 + 10 for i in range(365)],
            "WeatherSensitivity": [i % 50 + 5 for i in range(365)],
        }
    ),
    "Consumption_DataQuality": pd.DataFrame(
        {
            "Measurement": ["Quality A"] * 50 + ["Quality B"] * 50 + ["Quality C"] * 50,
            "Value": np.random.normal(loc=50, scale=10, size=150),
        }
    ),
    "Consumption_GeneralAnalysis": pd.DataFrame(
        {
            "Timestamp": pd.date_range(start="2023-01-01", periods=365, freq="D"),
            "Usage": np.random.normal(loc=500, scale=50, size=365).cumsum(),
            "Temperature": np.random.normal(loc=20, scale=5, size=365),
            "Pressure": np.random.normal(loc=30, scale=3, size=365),
        }
    ),
    "Consumption_UsageAnalysis": pd.DataFrame(
        {
            "Month": pd.date_range(start="2021-01-01", periods=12, freq="ME"),
            "Region": ["Region A"] * 6 + ["Region B"] * 6,
            "Usage": [500, 600, 550, 620, 640, 630, 580, 610, 590, 605, 615, 620],
        }
    ),
}

# plot_configs = m.get_analyses()

# Configuration dictionary defining UI components and plot settings for each dataframe


sample_plot_configs = {
    "DataQuality_DataQuality": {
        "BoxAndWhisker": {
            "title": "Water Data Quality Box and Whisker Plot",
            "x-axis": "Measurement",
            "y-axis": "Value",
            "x-axis_label": "Measurement Type",
            "y-axis_label": "Quality Value",
            "UI": {
                "dropdown": {
                    "instructions": "Select measurements to display:",
                    "controls": "Measurement",  # Specify the column to extract unique values from
                }
            },
            "dataframe": dataframes["Consumption_DataQuality"],
        }
    },
    "DataQuality_GeneralAnalysis": {
        "Timeseries": {
            "title": "Water Usage Over Time",
            "x-axis": "Timestamp",
            "y-axis": [
                "Usage",
                "Temperature",
                "Pressure",
            ],  # Multiple variables to plot
            "x-axis_label": "Date",
            "y-axis_label": "Values",
            "UI": {
                "datepicker": {
                    "html.Label": "Select Date Range",
                    "min_date_allowed": dataframes["Consumption_GeneralAnalysis"][
                        "Timestamp"
                    ]
                    .min()
                    .strftime("%Y-%m-%d"),
                    "max_date_allowed": dataframes["Consumption_GeneralAnalysis"][
                        "Timestamp"
                    ]
                    .max()
                    .strftime("%Y-%m-%d"),
                    "start_date": dataframes["Consumption_GeneralAnalysis"]["Timestamp"]
                    .min()
                    .strftime("%Y-%m-%d"),
                    "end_date": dataframes["Consumption_GeneralAnalysis"]["Timestamp"]
                    .max()
                    .strftime("%Y-%m-%d"),
                    "controls": "Timestamp",
                },
                "radioitem": {
                    "html.Label": "Select Frequency",
                    "options": [("Hourly", "h"), ("Daily", "D"), ("Monthly", "ME")],
                    "controls": "Timestamp",
                    "default_value": "D",
                },
            },
            "dataframe": dataframes["Consumption_GeneralAnalysis"],
        }
    },
    "DataQuality_UsageAnalysis": {
        "HeatMap": {
            "title": "Water Usage Heat Map",
            "x-axis": "Month",
            "y-axis": "Region",
            "z-axis": "Usage",
            "x-axis_label": "Month",
            "y-axis_label": "Region",
            "z-axis_label": "Water Usage (Liters)",
            "dataframe": dataframes["Consumption_UsageAnalysis"],
        }
    },
    "BuildingStructure_BuildingStructure": {
        "SunburstChart": {
            "title": "Building Structure Sunburst",
            "EntityID": "EntityID",
            "EntityType": "EntityType",
            "ParentID": "ParentID",
            "BuildingID": "BuildingID",
            "z-axis": "Level",
            "z-axis_label": "Hierarchy Level",
            "dataframe": dataframes["BuildingStructure_BuildingStructure"],
        }
    },
    "BuildingStructure_DataQuality": {
        "BoxAndWhisker": {
            "title": "Water Data Quality Box and Whisker Plot",
            "x-axis": "Measurement",
            "y-axis": "Value",
            "x-axis_label": "Measurement Type",
            "y-axis_label": "Quality Value",
            "UI": {
                "dropdown": {
                    "instructions": "Select measurements to display:",
                    "controls": "Measurement",  # Column to extract unique values from for dropdown
                }
            },
            "dataframe": dataframes["BuildingStructure_DataQuality"],
        }
    },
    # "ModelQuality_ClassInconsistency": {
    #     "PieChartAndTable": {
    #         "title": "Data Sources with Inconsistent Brick Class between Model and Mapper",
    #         "pie_charts": [
    #             {
    #                 "title": "Proportion of Consistent vs Inconsistent Classes",
    #                 "labels": "brick_class_is_consistent",
    #                 "textinfo": "percent+label",
    #                 "filter": None,
    #                 "dataframe": dataframes["ModelQuality_ClassInconsistency"],
    #             },
    #             {
    #                 "title": "Inconsistent Brick Classes by Class",
    #                 "labels": "brick_class",
    #                 "values": "count",
    #                 "textinfo": "percent+label",
    #                 "filter": "brick_class_is_consistent == False",
    #                 "dataframe": dataframes["ModelQuality_ClassInconsistency"],
    #             },
    #         ],
    #         "tables": [
    #             {
    #                 "title": "Data Sources with Inconsistent Brick Class",
    #                 "columns": [
    #                     "Brick Class in Model",
    #                     "Brick Class in Mapper",
    #                     "Entity ID",
    #                 ],
    #                 "data_source": "ModelQuality_ClassInconsistency",
    #                 "filter": "brick_class_is_consistent == False",
    #                 "rows": ["brick_class", "brick_class_in_mapping", "entity"],
    #                 "dataframe": dataframes["ModelQuality_ClassInconsistency"],
    #             }
    #         ],
    #     }
    # },
    # "ModelQuality_MissingTimeseries": {
    #     "PieChartAndTable": {
    #         "title": "Data Sources in Building Model without Timeseries Data",
    #         "pie_charts": [
    #             {
    #                 "title": "Proportion of Data Sources with Timeseries Data",
    #                 "labels": "has_data",
    #                 "textinfo": "percent+label",
    #                 "filter": None,
    #                 "dataframe": dataframes["ModelQuality_MissingTimeseries"],
    #             },
    #             {
    #                 "title": "Missing Timeseries Data by Class",
    #                 "labels": "brick_class",
    #                 "textinfo": "percent+label",
    #                 "filter": "has_data == False",
    #                 "dataframe": dataframes["ModelQuality_MissingTimeseries"],
    #             },
    #         ],
    #         "tables": [
    #             {
    #                 "title": "Data Sources with Missing Timeseries Data",
    #                 "columns": ["Brick Class", "Stream ID"],
    #                 "data_source": "ModelQuality_MissingTimeseries",
    #                 "filter": "has_data == False",
    #                 "rows": ["brick_class", "stream_id"],
    #                 "dataframe": dataframes["ModelQuality_ClassInconsistency"],
    #             },
    #             {
    #                 "title": "Data Sources with Available Timeseries Data",
    #                 "columns": ["Brick Class", "Stream ID"],
    #                 "data_source": "ModelQuality_MissingTimeseries",
    #                 "filter": "has_data == True",
    #                 "rows": ["brick_class", "stream_id"],
    #                 "dataframe": dataframes["ModelQuality_MissingTimeseries"],
    #             },
    #         ],
    #     }
    # },
    # "ModelQuality_RecognisedEntities": {
    #     "PieChartAndTable": {
    #         "title": "Brick Entities in Building Model Recognised by Brick Schema",
    #         "pie_charts": [
    #             {
    #                 "title": "Proportion of Recognised vs Unrecognised Entities",
    #                 "labels": "class_in_provided_brick",
    #                 "textinfo": "percent+label",
    #                 "filter": None,
    #                 "dataframe": dataframes["ModelQuality_RecognisedEntities"],
    #             },
    #             {
    #                 "title": "Unrecognised Entities by Class",
    #                 "labels": "brick_class",
    #                 "textinfo": "percent+label",
    #                 "filter": "class_in_provided_brick == False",
    #                 "dataframe": dataframes["ModelQuality_RecognisedEntities"],
    #             },
    #         ],
    #         "tables": [
    #             {
    #                 "title": "Unrecognised Entities",
    #                 "columns": ["Brick Class", "Entity ID"],
    #                 "data_source": "ModelQuality_RecognisedEntities",  # Main dataframe
    #                 "filter": "class_in_provided_brick == False",
    #                 "rows": ["brick_class", "entity_id"],
    #                 "dataframe": dataframes["ModelQuality_RecognisedEntities"],
    #             },
    #             {
    #                 "title": "Recognised Entities",
    #                 "columns": ["Brick Class", "Entity ID"],
    #                 "data_source": "ModelQuality_RecognisedEntities",
    #                 "filter": "class_in_provided_brick == True",
    #                 "rows": ["brick_class", "entity_id"],
    #                 "dataframe": dataframes["ModelQuality_RecognisedEntities"],
    #             },
    #         ],
    #     }
    # },
    # "RoomClimate_Rooms": {
    #     "TableAndTimeseries": {
    #         "title": "Room Climate",
    #         "table": {
    #             "title": "List of Rooms with Air Temperature Sensors and Setpoints",
    #             "columns": [
    #                 "Room Class",
    #                 "Room ID",
    #             ],
    #             "rows": ["room_class", "room_id"],
    #             "filter": None,
    #             "dataframe": pd.DataFrame(
    #                 {
    #                     "room_class": [
    #                         "Conference Room",
    #                         "Conference Room",
    #                         "Library",
    #                         "Office",
    #                     ],
    #                     "room_id": ["Room 1", "Room 2", "Room 3", "Room 4"],
    #                 }
    #             ),
    #         },
    #         "timeseries": [
    #             {
    #                 "title": "Conference Room",
    #                 "dataframe": pd.DataFrame(
    #                     {
    #                         "Date": pd.date_range(
    #                             start="2021-01-01", periods=365, freq="D"
    #                         ),
    #                         "Air_Temperature_Sensor": np.random.normal(15, 3, 365),
    #                         "Room_Air_Temperature_Setpoint": np.random.normal(
    #                             15, 1, 365
    #                         ),
    #                         "Outside_Air_Temperature_Sensor": np.random.normal(
    #                             15, 5, 365
    #                         ),
    #                     }
    #                 ),
    #             },
    #             {
    #                 "title": "Conference Room",
    #                 "dataframe": pd.DataFrame(
    #                     {
    #                         "Date": pd.date_range(
    #                             start="2021-01-01", periods=365, freq="D"
    #                         ),
    #                         "Air_Temperature_Sensor": np.random.normal(15, 3, 365),
    #                         "Room_Air_Temperature_Setpoint": np.random.normal(
    #                             15, 1, 365
    #                         ),
    #                         "Outside_Air_Temperature_Sensor": np.random.normal(
    #                             15, 5, 365
    #                         ),
    #                     }
    #                 ),
    #             },
    #             {
    #                 "title": "Library",
    #                 "dataframe": pd.DataFrame(
    #                     {
    #                         "Date": pd.date_range(
    #                             start="2021-01-01", periods=365, freq="D"
    #                         ),
    #                         "Air_Temperature_Sensor": np.random.normal(15, 3, 365),
    #                         "Room_Air_Temperature_Setpoint": np.random.normal(
    #                             15, 1, 365
    #                         ),
    #                         "Outside_Air_Temperature_Sensor": np.random.normal(
    #                             15, 5, 365
    #                         ),
    #                     }
    #                 ),
    #             },
    #             {
    #                 "title": "Office",
    #                 "dataframe": pd.DataFrame(
    #                     {
    #                         "Date": pd.date_range(
    #                             start="2021-01-01", periods=365, freq="D"
    #                         ),
    #                         "Air_Temperature_Sensor": np.random.normal(15, 3, 365),
    #                         "Room_Air_Temperature_Setpoint": np.random.normal(
    #                             15, 1, 365
    #                         ),
    #                         "Outside_Air_Temperature_Sensor": np.random.normal(
    #                             15, 5, 365
    #                         ),
    #                     }
    #                 ),
    #             },
    #         ],
    #     }
    # },
    # "RoomClimate_WeatherSensitivity": {
    #     "SurfacePlot": {
    #         "title": "Temperature vs Weather Sensitivity",
    #         "X-value": "Day",
    #         "Y-values": ["Hour"],
    #         "Z-value": "WeatherSensitivity",
    #         "x-axis_label": "Day",
    #         "y-axis_label": "Hour of the Day",
    #         "z-axis_label": "Weather Sensitivity",
    #         "dataframe": dataframes["RoomClimate_WeatherSensitivity"],
    #     }
    # },
    "Consumption_Raw": {
        "BoxAndWhisker": {
            "title": "Water Data Quality Box and Whisker Plot",
            "x-axis": "Measurement",
            "y-axis": "Value",
            "x-axis_label": "Measurement Type",
            "y-axis_label": "Quality Value",
            "UI": {
                "dropdown": {
                    "instructions": "Select measurements to display:",
                    "controls": "Measurement",  # Specify the column to extract unique values from
                }
            },
            "dataframe": dataframes["Consumption_DataQuality"],
        }
    },
    # "Consumption_WeatherSensitivity": {
    #     "Timeseries": {
    #         "title": "Water Usage Over Time",
    #         "x-axis": "Timestamp",
    #         "y-axis": [
    #             "Usage",
    #             "Temperature",
    #             "Pressure",
    #         ],  # Multiple variables to plot
    #         "x-axis_label": "Date",
    #         "y-axis_label": "Values",
    #         "UI": {
    #             "datepicker": {
    #                 "html.Label": "Select Date Range",
    #                 "min_date_allowed": dataframes["Consumption_GeneralAnalysis"][
    #                     "Timestamp"
    #                 ]
    #                 .min()
    #                 .strftime("%Y-%m-%d"),
    #                 "max_date_allowed": dataframes["Consumption_GeneralAnalysis"][
    #                     "Timestamp"
    #                 ]
    #                 .max()
    #                 .strftime("%Y-%m-%d"),
    #                 "start_date": dataframes["Consumption_GeneralAnalysis"]["Timestamp"]
    #                 .min()
    #                 .strftime("%Y-%m-%d"),
    #                 "end_date": dataframes["Consumption_GeneralAnalysis"]["Timestamp"]
    #                 .max()
    #                 .strftime("%Y-%m-%d"),
    #                 "controls": "Timestamp",
    #             },
    #             "radioitem": {
    #                 "html.Label": "Select Frequency",
    #                 "options": [("Hourly", "h"), ("Daily", "D"), ("Monthly", "ME")],
    #                 "controls": "Timestamp",
    #                 "default_value": "D",
    #             },
    #         },
    #         "dataframe": dataframes["Consumption_GeneralAnalysis"],
    #     }
    # },
    # "Consumption_UsageAnalysis": {
    #     "HeatMap": {
    #         "title": "Water Usage Heat Map",
    #         "x-axis": "Month",
    #         "y-axis": "Region",
    #         "z-axis": "Usage",
    #         "x-axis_label": "Month",
    #         "y-axis_label": "Region",
    #         "z-axis_label": "Water Usage (Liters)",
    #         "dataframe": dataframes["Consumption_UsageAnalysis"],
    #     }
    # },
    # "NoSubcategory": { #@tim: FIXME: make sure having this doesn't break anything
    #     "HeatMap": {
    #         "title": "Water Usage Heat Map",
    #         "x-axis": "Month",
    #         "y-axis": "Region",
    #         "z-axis": "Usage",
    #         "x-axis_label": "Month",
    #         "y-axis_label": "Region",
    #         "z-axis_label": "Water Usage (Liters)",
    #         "dataframe": dataframes["Consumption_UsageAnalysis"],
    #     }
    # },
}


# sample_plot_configs |= data
# print(sample_plot_configs)


################################################################################
#                                    LAYOUT                                    #
################################################################################


def construct_layout():

    # Create a mapping for dataframes and plot configurations based on URLs
    global url_to_key_mapping
    url_to_key_mapping = create_url_mapping(plot_configs)

    global bottom_buttons
    # Define the bottom buttons for downloading reports and data
    bottom_buttons = dbc.Row(
        [
            dbc.Col(
                dbc.Button(
                    "Download Report",
                    id="download-left",
                    color="success",
                ),
                width="auto",
            ),
            dbc.Col(
                dbc.Button("Download Data", id="download-right", color="success"),
                width="auto",
            ),
        ],
        justify="end",
        className="ml-auto",
    )

    # Create main categories and subcategories from the dataframe keys
    categories = create_category_structure(plot_configs.keys())

    # Special case: Add "Home" as a top-level category without subcategories
    categories["Home"] = []  # "Home" has no subcategories or associated data

    # Generate the sidebar component
    sidebar = html.Div(
        [
            html.Button(
                html.Div(
                    html.Img(src="/assets/logo.svg", className="sidebar-logo"),
                    className="sidebar-logo-container",
                ),
                id="logo-button",
                n_clicks=0,
                className="logo-button",
                style={
                    "background": "none",
                    "border": "none",
                    "padding": "0",
                    "margin": "0",
                    "cursor": "pointer",
                },
            ),
            html.Hr(),
            generate_sidebar(categories),
        ],
        className="sidebar",
    )

    # Content area where page-specific content will be displayed
    content = html.Div(id="page-content", className="content")

    # Define the overall layout of the app, including sidebar and content area
    app.layout = html.Div(
        [
            dcc.Location(id="url"),
            sidebar,
            dbc.Spinner(
                children=[content],
                color="#3c9639",
                fullscreen=False,
                type="border",
                size="md",
            ),
            dcc.Store(id="screen-size", storage_type="session"),
            dbc.Modal(
                [
                    dbc.ModalHeader(dbc.ModalTitle("Navigate to Homepage?")),
                    dbc.ModalBody("Are you sure you want to go to the homepage?"),
                    dbc.ModalFooter(
                        [
                            dbc.Button(
                                "Yes",
                                id="modal-yes-button",
                                color="success",
                                n_clicks=0,
                            ),
                            dbc.Button(
                                "No",
                                id="modal-no-button",
                                outline=True,
                                color="success",
                                n_clicks=0,
                            ),
                        ]
                    ),
                ],
                id="warning-modal",
                is_open=False,
            ),
            dcc.Download(id="download-data-csv"),
            dcc.Download(id="download-report-pdf"),
        ]
    )


# Function to generate the sidebar navigation based on categories
def generate_sidebar(categories):
    nav_links = []

    # Add "Home" link at the top of the sidebar
    if "Home" in categories:
        nav_links.append(dbc.NavLink("Home", href="/", active="exact"))

    # Loop through categories and generate links
    for category, subcategories in categories.items():
        if category != "Home":  # Skip "Home" as it's already added
            nav_links.append(
                dbc.NavLink(
                    category,
                    href=f"/{category.lower().replace(' ', '-')}",
                    active="exact",
                )
            )

    return dbc.Nav(nav_links, vertical=True, pills=True)


# Callback to control the modal window and handle navigation when the logo is clicked
@app.callback(
    [Output("warning-modal", "is_open"), Output("url", "pathname")],
    [
        Input("logo-button", "n_clicks"),
        Input("modal-yes-button", "n_clicks"),
        Input("modal-no-button", "n_clicks"),
    ],
    [State("warning-modal", "is_open"), State("url", "pathname")],
)
def toggle_modal_and_navigate(
    n_clicks_logo, n_clicks_yes, n_clicks_no, is_open, current_pathname
):
    ctx = (
        dash.callback_context
    )  # Context to determine which input triggered the callback

    if not ctx.triggered:
        return is_open, dash.no_update
    else:
        button_id = ctx.triggered[0]["prop_id"].split(".")[0]

    # Open modal when logo button is clicked
    if button_id == "logo-button" and n_clicks_logo:
        return True, dash.no_update  # Open modal without changing the URL

    # Navigate to homepage if "Yes" button in the modal is clicked
    elif button_id == "modal-yes-button" and n_clicks_yes:
        return False, "/"  # Close modal and navigate to "/"

    # Close modal if "No" button is clicked
    elif button_id == "modal-no-button" and n_clicks_no:
        return False, current_pathname  # Close modal, stay on current page

    return is_open, dash.no_update


# Callback to update the main content area based on the current URL path
@app.callback(Output("page-content", "children"), Input("url", "pathname"))
def display_page(pathname):
    # Handle the homepage when the URL is "/" or an empty path
    if pathname == "/" or pathname == "":
        return html.Div(
            [html.H1("Welcome to the Homepage!"), html.P("This is a static homepage.")]
        )
    # Clean and split the path for category and subcategory
    cleaned_path = pathname.strip("/").lower().split("/")

    # Standardise the main category by removing hyphens
    main_category = cleaned_path[0].replace("-", "") if len(cleaned_path) > 0 else None

    # If no matching category is found, display a 404 error page
    if main_category not in url_to_key_mapping:
        return html.Div([html.H1("404 Page not found")])

    subcategories = url_to_key_mapping[main_category]

    if subcategories:
        # Create tabs for categories with subcategories
        tabs = []
        tab_ids = []

        for subcategory_key in subcategories:
            config = plot_configs[subcategory_key]

            # Extract the subcategory (after the underscore) for labeling
            subcategory_label = (
                pascal_to_words(subcategory_key.split("_")[1])
                if "_" in subcategory_key
                else "Main"
            )

            # Create tabs for each plot type (e.g., HeatMap, SunburstChart, etc.)
            for plot_type, plot_settings in config.items():
                plot_id = f"{subcategory_key}-{plot_type}"

                # Pass the subcategory as the tab label
                tab_content = create_tab_content(
                    plot_type, plot_settings, plot_id, subcategory_label
                )

                if tab_content:
                    tabs.append(tab_content)
                    tab_ids.append(plot_id)

        # Return the tabs within a container if at least one tab was created
        if len(tabs) >= 1:
            return dbc.Container(
                [
                    dbc.Tabs(tabs, id="tabs", active_tab=tab_ids[0]),
                    html.Hr(),
                    bottom_buttons,  # Add the buttons below the tabs
                ],
                fluid=True,
            )


# Function to create tab content with the graph and UI controls for a given plot type
def create_tab_content(plot_type, plot_settings, plot_id, subcategory):
    # @tim: FIXME: see if this can be generalised
    if plot_type == "PieChartAndTable":
        return create_pie_chart_and_table_tab(plot_settings, plot_id, subcategory)
    elif plot_type == "TableAndTimeseries":
        return create_table_and_timeseries_tab(plot_settings, plot_id, subcategory)
    elif plot_type == "TimeseriesAndTable":
        return create_timeseries_and_table_tab(plot_settings, plot_id, subcategory)

    else:
        # Handle other plot types
        figure = create_plot(plot_type, plot_settings)
        if not figure:
            return None  # Skip if the plot figure couldn't be created

        # Determine which UI components to add based on plot type
        if plot_type == "HeatMap":
            ui = create_ui_for_heatmap(plot_type, plot_id)
            graph_type = "heatmap-graph"
        elif plot_type == "SunburstChart":
            ui = create_ui_for_sunburst(plot_type, plot_id)
            graph_type = "sunburst-graph"
        elif plot_type == "SurfacePlot":
            ui = create_ui_for_surface_plot(plot_type, plot_id)
            graph_type = "surface-graph"
        elif plot_type == "BoxAndWhisker":
            ui = create_ui_for_box_and_whisker(plot_type, plot_id, plot_settings)
            graph_type = "box-and-whisker-graph"
        elif plot_type == "Timeseries":
            ui = create_ui_for_timeseries(plot_type, plot_id, plot_settings)
            graph_type = "timeseries-graph"
        else:
            ui = None
            graph_type = f"{plot_type.lower()}-graph"  # Generic graph type

        # Create the tab with the graph and UI components
        return dbc.Tab(
            dbc.Container(
                [
                    dcc.Graph(id={"type": graph_type, "index": plot_id}, figure=figure),
                    ui,  # Add UI controls below the graph
                ]
            ),
            label=subcategory,  # Label for the tab
            tab_id=plot_id,
        )


# Creates the appropriate plot based on plot type and settings.
def create_plot(plot_type, plot_settings):
    data = plot_settings["dataframe"]
    title = plot_settings["title"]

    if plot_type == "HeatMap":
        # Create a heatmap figure
        return create_heatmap(
            data,
            x_column=plot_settings["x-axis"],
            y_column=plot_settings["y-axis"],
            z_column=plot_settings["z-axis"],
            color_scale="Viridis",  # Default color scale
            title=title,
            x_label=plot_settings["x-axis_label"],
            y_label=plot_settings["y-axis_label"],
            z_label=plot_settings["z-axis_label"],
        )

    elif plot_type == "SunburstChart":
        # Create a sunburst chart figure
        return create_sunburst_chart(
            data,
            building_column=plot_settings["BuildingID"],
            parent_column=plot_settings["ParentID"],
            entity_column=plot_settings["EntityID"],
            value_column=plot_settings["z-axis"],
            title=title,
            color_scale="Viridis",  # Default color scale
        )

    elif plot_type == "SurfacePlot":
        # Create a surface plot figure
        X = data[plot_settings["X-value"]]
        Y = data[plot_settings["Y-values"][0]]  # Using the first Y-column
        Z = data[plot_settings["Z-value"]]
        return create_surface_plot(
            X=X,
            Y=Y,
            Z=Z,
            color_scale="Viridis",
            title=title,
            x_label=plot_settings["x-axis_label"],
            y_label=plot_settings["y-axis_label"],
            z_label=plot_settings["z-axis_label"],
        )

    elif plot_type == "BoxAndWhisker":
        # Create a box and whisker plot figure
        return create_box_plot(
            data=data,
            title=title,
            x_label=plot_settings["x-axis_label"],
            y_label=plot_settings["y-axis_label"],
        )

    elif plot_type == "Timeseries":
        # Create a timeseries (line) plot figure
        return create_line_plot(
            data=data,
            x_column=plot_settings["x-axis"],
            y_columns=plot_settings["y-axis"],
            title=title,
        )

    return None  # Return None if the plot type doesn't match expected ones


################################################################################
#                               HELPER FUNCTIONS                               #
################################################################################


# Creates a mapping from URL-friendly paths to dataframe keys. For example, it turns 'Consumption_UsageAnalysis' into 'water'.
def create_url_mapping(plot_configs):
    url_mapping = defaultdict(list)

    for key in plot_configs.keys():
        # Convert PascalCase to URL-friendly lowercase path, only using the first part (before '_')
        url_friendly_key = key.split("_")[0].lower()
        # Map the URL-friendly path to the original key
        url_mapping[url_friendly_key].append(key)

    return url_mapping


# Filters the dataframe based on the provided date range and resamples it according to frequency.
def filter_data(df, start_date, end_date, variables, frequency=None):
    df_filtered = df[(df["Timestamp"] >= start_date) & (df["Timestamp"] <= end_date)]
    if frequency:
        df_filtered = (
            df_filtered.set_index("Timestamp").resample(frequency).mean().reset_index()
        )
    return df_filtered[["Timestamp"] + variables]


# Helper function to convert PascalCase text to readable words
def pascal_to_words(text):
    return re.sub(r"(?<!^)(?=[A-Z])", " ", text)


# Helper function to create a hierarchical category structure from analysis keys
def create_category_structure(analysis_list):
    categories = {}
    for analysis_name in analysis_list:
        if "_" in analysis_name:
            main_cat, sub_cat = analysis_name.split(
                "_", 1
            )  # Split into main category and subcategory
        else:
            main_cat, sub_cat = analysis_name, None  # If no subcategory, set to None

        main_cat = pascal_to_words(main_cat)  # Convert to readable format
        sub_cat = (
            pascal_to_words(sub_cat) if sub_cat else None
        )  # Convert subcategory if exists

        # Add to dictionary
        if main_cat not in categories:
            categories[main_cat] = []  # Initialise subcategory list
        if sub_cat:
            categories[main_cat].append(sub_cat)  # Add subcategory if exists

    return categories


################################################################################
#                                VISUALISATIONS                                #
################################################################################

# --------------------------------  BOX PLOT  -------------------------------- #


# Function to create box and whiskers plot
def create_box_plot(data, title, x_label, y_label):
    fig = px.box(data, x="Measurement", y="Value")
    fig.update_layout(
        title={"text": title, "x": 0.5, "xanchor": "center"},
        font_color="black",
        plot_bgcolor="white",
        margin=dict(l=50, r=50, b=100, t=50),
        autosize=True,
        font=dict(size=10),
        legend=dict(font=dict(size=10)),
    )
    fig.update_xaxes(
        mirror=True,
        ticks="outside",
        showline=True,
        linecolor="black",
        gridcolor="lightgrey",
    )
    fig.update_yaxes(
        mirror=True,
        ticks="outside",
        showline=True,
        linecolor="black",
        gridcolor="lightgrey",
    )
    fig.update_traces(marker_color="#3c9639")
    return fig


# Function to dynamically create UI components for BoxAndWhisker plots
def create_ui_for_box_and_whisker(plot_type, plot_id, plot_settings):
    data = plot_settings["dataframe"]

    if plot_type == "BoxAndWhisker":
        ui_elements = []
        dropdown_config = plot_settings.get("UI", {}).get("dropdown", {})
        instructions = dropdown_config.get("instructions", "")
        controls_column = dropdown_config.get("controls", "")

        # Extract unique values from the specified controls column
        if controls_column and controls_column in data.columns:
            unique_vars = data[controls_column].unique()
            if len(unique_vars) > 1:
                ui_elements.append(html.Label(instructions))
                ui_elements.append(
                    dcc.Dropdown(
                        id={"type": "box-and-whisker-dropdown", "index": plot_id},
                        options=[{"label": var, "value": var} for var in unique_vars],
                        value=list(unique_vars),  # Default to all variables
                        multi=True,
                        clearable=False,
                    )
                )
        return html.Div(ui_elements)
    return None


# Callback to update BoxAndWhisker plots based on selected measurements
@app.callback(
    Output({"type": "box-and-whisker-graph", "index": MATCH}, "figure"),
    Input({"type": "box-and-whisker-dropdown", "index": MATCH}, "value"),
    State({"type": "box-and-whisker-dropdown", "index": MATCH}, "id"),
    prevent_initial_call=True,
)
def update_box_and_whisker(selected_variables, input_id):
    plot_id = input_id["index"]  # Extract the unique plot identifier
    category_key, plot_type = plot_id.rsplit(
        "-", 1
    )  # Split to get dataframe key and plot type
    data = dataframes[category_key]  # Retrieve the relevant dataframe
    plot_settings = plot_configs[category_key][plot_type]  # Get plot settings

    # If no variables are selected, default to all available measurements
    if not selected_variables:
        controls_column = (
            plot_settings.get("UI", {}).get("dropdown", {}).get("controls", "")
        )
        if controls_column and controls_column in data.columns:
            selected_variables = data[controls_column].unique()
        else:
            selected_variables = []

    # Filter data based on selected variables
    filtered_data = data[data["Measurement"].isin(selected_variables)]

    # Create an updated box and whisker plot with the filtered data
    updated_figure = create_box_plot(
        data=filtered_data,
        title=plot_settings["title"],
        x_label=plot_settings["x-axis_label"],
        y_label=plot_settings["y-axis_label"],
    )

    return updated_figure


# --------------------------------  HEATMAP  --------------------------------- #


# Create heatmap function
def create_heatmap(
    data, x_column, y_column, z_column, color_scale, title, x_label, y_label, z_label
):
    # fig = px.density_heatmap(
    #     data, x=x_column, y=y_column, z=z_column, color_continuous_scale=color_scale
    # )
    fig = go.Figure(
        data=go.Heatmap(
            x=data[x_column], y=data[y_column], z=data[z_column], colorscale=color_scale
        )
    )
    fig.update_layout(
        title={"text": title, "x": 0.5, "xanchor": "center"},
        xaxis_title=x_label,
        yaxis_title=y_label,
        font_color="black",
        plot_bgcolor="white",
        coloraxis_colorbar=dict(
            title=z_label,
            orientation="h",
            yanchor="top",
            y=-0.4,
            xanchor="center",
            x=0.5,
            title_side="bottom",
        ),
    )

    fig.update_xaxes(
        mirror=True,
        ticks="outside",
        showline=True,
        linecolor="black",
        gridcolor="lightgrey",
    )
    fig.update_yaxes(
        mirror=True,
        ticks="outside",
        showline=True,
        linecolor="black",
        gridcolor="lightgrey",
    )

    return fig


# Function to dynamically create UI specifically for HeatMap plot types
def create_ui_for_heatmap(plot_type, plot_id):
    if plot_type == "HeatMap":
        return html.Div(
            [
                html.Label("Select Color Scale"),
                dcc.Dropdown(
                    id={
                        "type": "heatmap-color-scale-dropdown",
                        "index": plot_id,
                    },  # Use pattern-matching ID
                    options=[
                        {"label": scale, "value": scale}
                        for scale in px.colors.named_colorscales()
                    ],
                    value="Viridis",
                    clearable=False,
                ),
            ]
        )
    return None


# Callback to update HeatMap figures based on selected color scale
@app.callback(
    Output({"type": "heatmap-graph", "index": MATCH}, "figure"),
    Input({"type": "heatmap-color-scale-dropdown", "index": MATCH}, "value"),
    State({"type": "heatmap-color-scale-dropdown", "index": MATCH}, "id"),
    prevent_initial_call=True,
)
def update_heatmap(selected_color_scale, input_id):
    plot_id = input_id["index"]  # Extract the unique plot identifier
    category_key, plot_type = plot_id.rsplit(
        "-", 1
    )  # Split to get dataframe key and plot type
    data = plot_configs[category_key]["HeatMap"][
        "dataframe"
    ]  # Retrieve the relevant dataframe
    plot_settings = plot_configs[category_key][plot_type]  # Get plot settings

    # Create an updated heatmap with the new color scale
    updated_figure = create_heatmap(
        data=data,
        x_column=plot_settings["x-axis"],
        y_column=plot_settings["y-axis"],
        z_column=plot_settings["z-axis"],
        color_scale=selected_color_scale,
        title=plot_settings["title"],
        x_label=plot_settings["x-axis_label"],
        y_label=plot_settings["y-axis_label"],
        z_label=plot_settings["z-axis_label"],
    )

    return updated_figure


# -------------------------  LINE PLOT / TIMESERIES  ------------------------- #


# Function to create line plot (timeseries)
def create_line_plot(data, x_column, y_columns, title):
    fig = px.line(data, x=x_column, y=y_columns, markers=True)
    fig.update_layout(
        title={"text": title, "x": 0.5, "xanchor": "center"},
        font_color="black",
        plot_bgcolor="white",
        legend=dict(orientation="h", yanchor="top", y=-0.3, xanchor="center", x=0.5),
    )
    fig.update_xaxes(
        mirror=True,
        ticks="outside",
        showline=True,
        linecolor="black",
        gridcolor="lightgrey",
    )
    fig.update_yaxes(
        mirror=True,
        ticks="outside",
        showline=True,
        linecolor="black",
        gridcolor="lightgrey",
    )
    return fig


# Function to dynamically create UI components for Timeseries plots
def create_ui_for_timeseries(plot_type, plot_id, plot_settings):
    if plot_type == "Timeseries":
        ui_elements = []
        ui_config = plot_settings.get("UI", {})

        # Date Picker
        datepicker_config = ui_config.get("datepicker", {})
        if datepicker_config:
            ui_elements.append(
                html.Label(datepicker_config.get("html.Label", "Select Date Range"))
            )
            ui_elements.append(
                dcc.DatePickerRange(
                    id={"type": "timeseries-datepicker", "index": plot_id},
                    min_date_allowed=datepicker_config.get("min_date_allowed"),
                    max_date_allowed=datepicker_config.get("max_date_allowed"),
                    start_date=datepicker_config.get("start_date"),
                    end_date=datepicker_config.get("end_date"),
                    display_format="YYYY-MM-DD",
                )
            )

        # Radio Items for Frequency
        radioitem_config = ui_config.get("radioitem", {})
        if radioitem_config:
            ui_elements.append(html.Br())
            ui_elements.append(
                html.Label(radioitem_config.get("html.Label", "Select Frequency"))
            )
            ui_elements.append(
                dcc.RadioItems(
                    id={"type": "timeseries-radioitem", "index": plot_id},
                    options=[
                        {"label": label, "value": value}
                        for label, value in radioitem_config.get("options", [])
                    ],
                    value=radioitem_config.get("default_value", "D"),
                    labelStyle={"display": "inline-block", "margin-right": "10px"},
                )
            )

        return html.Div(ui_elements, style={"margin-top": "20px"})
    return None


# Callback to update Timeseries plots based on selected date range and frequency
@app.callback(
    Output({"type": "timeseries-graph", "index": MATCH}, "figure"),
    [
        Input({"type": "timeseries-datepicker", "index": MATCH}, "start_date"),
        Input({"type": "timeseries-datepicker", "index": MATCH}, "end_date"),
        Input({"type": "timeseries-radioitem", "index": MATCH}, "value"),
    ],
    State({"type": "timeseries-graph", "index": MATCH}, "id"),
    prevent_initial_call=True,
)
def update_timeseries_plot(start_date, end_date, frequency, input_id):
    plot_id = input_id["index"]  # Extract the unique plot identifier
    category_key, plot_type = plot_id.rsplit(
        "-", 1
    )  # Split to get dataframe key and plot type
    data = dataframes[category_key]  # Retrieve the relevant dataframe
    plot_settings = plot_configs[category_key][plot_type]  # Get plot settings

    # Variables to plot on the y-axis
    y_columns = plot_settings["y-axis"]

    # Filter data based on selected date range and resample based on frequency
    filtered_data = filter_data(
        df=data,
        start_date=pd.to_datetime(start_date),
        end_date=pd.to_datetime(end_date),
        variables=y_columns,
        frequency=frequency,  # Resampling frequency (e.g., 'D' for daily)
    )

    # Create an updated timeseries plot with the filtered and resampled data
    updated_figure = create_line_plot(
        data=filtered_data,
        x_column=plot_settings["x-axis"],
        y_columns=y_columns,
        title=plot_settings["title"],
    )

    return updated_figure


# -------------------------------  PIE CHART  -------------------------------- #


# Create pie chart function
def create_pie_chart(
    data,
    labels_column,
    values_column,
    title,
    textinfo="percent+label",
    showlegend=False,
):
    # Calculate the count of occurrences for each label
    value_counts = data[labels_column].value_counts().reset_index()
    value_counts.columns = [labels_column, "count"]  # Rename for clarity

    fig = go.Figure(
        go.Pie(
            labels=value_counts[labels_column],
            values=value_counts["count"],
            textinfo=textinfo,
            textposition="inside",  # @tim: FIXME: make this an actual parameter
            showlegend=showlegend,  # @tim: FIXME: make this an actual parameter
        )
    )

    fig.update_layout(
        title={"text": title, "x": 0.5, "xanchor": "center"},
        font_color="black",
        plot_bgcolor="white",
        height=500,  # @tim: FIXME: make this an actual parameter or auto-size?
    )

    return fig


# ---------------------------  PIE CHART + TABLE  ---------------------------- #


# Creates a Dash Tab containing pie charts and tables based on the provided settings.
def create_pie_chart_and_table_tab(plot_settings, plot_id, subcategory):
    # Extract pie charts and tables configurations
    pie_charts = plot_settings.get("pie_charts", [])
    tables = plot_settings.get("tables", [])

    pie_content = []
    # Create pie charts
    for pie_chart in pie_charts:
        data = pie_chart["dataframe"]
        chart_title = pie_chart.get("title", "Pie Chart")
        chart_labels = pie_chart.get("labels", "")
        textinfo = pie_chart.get("textinfo", "percent+label")
        filter_condition = pie_chart.get("filter", None)  # Get filter if present

        # Apply filter if specified
        if filter_condition:
            try:
                filtered_data = data.query(filter_condition)
            except Exception as e:
                print(
                    f"Error applying filter '{filter_condition}' on dataframe '{plot_id}': {e}"
                )
                filtered_data = data  # Fallback to unfiltered data if there's an error
        else:
            filtered_data = data

        # Create pie chart figure
        pie_figure = create_pie_chart(
            data=filtered_data,
            labels_column=chart_labels,
            values_column=None,  # 'values' is not used in create_pie_chart
            title=chart_title,
            textinfo=textinfo,
        )

        pie_content.append(
            dbc.Col(
                dcc.Graph(figure=pie_figure),
                width=6,  # Adjust width as needed
                className="mb-4",  # Add margin-bottom for spacing
            )
        )

    table_content = []
    # Create tables with applied filters
    for table in tables:
        data = table["dataframe"]
        table_title = table.get("title", "Table")
        columns = table.get("columns", [])
        filter_condition = table.get("filter", None)
        rows = table.get("rows", columns)

        if filter_condition:
            # Apply the filter using pandas query
            try:
                filtered_data = data.query(filter_condition)
            except Exception as e:
                print(
                    f"Error applying filter '{filter_condition}' on dataframe '{plot_id}': {e}"
                )
                filtered_data = data  # Fallback to unfiltered data if there's an error
        else:
            filtered_data = data

        # Select and rename columns as needed
        selected_columns = rows
        # Ensure all selected columns exist in the dataframe
        existing_columns = [
            col for col in selected_columns if col in filtered_data.columns
        ]
        if not existing_columns:
            print(
                f"No matching columns found for table '{table_title}' in dataframe '{plot_id}'."
            )
            continue  # Skip creating this table

        display_data = filtered_data[existing_columns].rename(
            columns=dict(
                zip(selected_columns, columns)
            )  # Rename to user-friendly names
        )

        # Create and append the table
        table_content.append(
            dbc.Col(
                create_table(display_data, columns, table_title),
                width=12,  # Full-width for each table
                className="mb-4",  # Add margin-bottom for spacing
            )
        )

    # Combine pie charts and tables into layout
    content = []
    content.append(html.H2(plot_settings["title"], className="text-center"))
    if pie_content:
        content.append(dbc.Row(pie_content, justify="center"))
    if table_content:
        content.append(html.Hr())  # Append the horizontal rule first
        content.append(dbc.Row(table_content))

    # Combine all elements into a single tab
    return dbc.Tab(
        dbc.Container(content, fluid=True, className="py-4"),
        label=subcategory,
        tab_id=plot_id,
    )


# Creates a Dash Tab containing a table and a timeseries plot based on the selected row.
def create_table_and_timeseries_tab(plot_settings, plot_id, subcategory):
    # Extract pie charts and tables configurations
    table = plot_settings.get("table", [])
    timeseries = plot_settings.get("timeseries", [])

    content = []
    content.append(html.H2(plot_settings["title"], className="text-center"))

    data = table["dataframe"]
    table_title = table.get("title", "Table")
    columns = table.get("columns", [])
    filter_condition = table.get("filter", None)
    rows = table.get("rows", columns)

    if filter_condition:
        # Apply the filter using pandas query
        try:
            filtered_data = data.query(filter_condition)
        except Exception as e:
            print(
                f"Error applying filter '{filter_condition}' on dataframe '{plot_id}': {e}"
            )
            filtered_data = data  # Fallback to unfiltered data if there's an error
    else:
        filtered_data = data

    # Select and rename columns as needed
    selected_columns = rows
    # Ensure all selected columns exist in the dataframe
    existing_columns = [col for col in selected_columns if col in filtered_data.columns]
    if not existing_columns:
        print(
            f"No matching columns found for table '{table_title}' in dataframe '{plot_id}'."
        )
        return  # Skip creating this table

    display_data = filtered_data[existing_columns].rename(
        columns=dict(zip(selected_columns, columns))  # Rename to user-friendly names
    )

    # First add the graph on top
    content.append(dcc.Graph(id="updateable-line-chart"))

    # content.append(dbc.Row(table_content))
    content.append(html.Hr())  # Append the horizontal rule first

    # Then create and append the table
    content.append(
        dbc.Col(
            create_table(
                display_data,
                columns,
                table_title,
                # id=f"{plot_id}-table",
                id="datatable",
                # id=table["id"],
                row_selectable="single",
                selected_rows=[0],
                sort_action="none",
            ),
            width=12,  # Full-width for each table
            className="mb-4",  # Add margin-bottom for spacing
        )
    )

    # Combine all elements into a single tab
    return dbc.Tab(
        dbc.Container(content, fluid=True, className="py-4"),
        label=subcategory,
        tab_id=plot_id,
    )


# Creates a Dash Tab containing a table and a timeseries plot based on the selected row.
def create_timeseries_and_table_tab(plot_settings, plot_id, subcategory):
    # Extract pie charts and tables configurations
    table = plot_settings.get("table", [])
    timeseries = plot_settings.get("timeseries", [])

    content = []
    content.append(html.H2(plot_settings["title"], className="text-center"))

    data = table["dataframe"]
    table_title = table.get("title", "Table")
    columns = table.get("columns", [])
    filter_condition = table.get("filter", None)
    rows = table.get("rows", columns)

    if filter_condition:
        # Apply the filter using pandas query
        try:
            filtered_data = data.query(filter_condition)
        except Exception as e:
            print(
                f"Error applying filter '{filter_condition}' on dataframe '{plot_id}': {e}"
            )
            filtered_data = data  # Fallback to unfiltered data if there's an error
    else:
        filtered_data = data

    # Select and rename columns as needed
    selected_columns = rows
    # Ensure all selected columns exist in the dataframe
    existing_columns = [col for col in selected_columns if col in filtered_data.columns]
    if not existing_columns:
        print(
            f"No matching columns found for table '{table_title}' in dataframe '{plot_id}'."
        )
        return  # Skip creating this table

    display_data = filtered_data[existing_columns].rename(
        columns=dict(zip(selected_columns, columns))  # Rename to user-friendly names
    )

    # First add the graph on top
    content.append(dcc.Graph(id="streams-updateable-line-chart"))

    # content.append(dbc.Row(table_content))
    content.append(html.Hr())  # Append the horizontal rule first

    # Then create and append the table
    content.append(
        dbc.Col(
            create_table(
                display_data,
                columns,
                table_title,
                # id=f"{plot_id}-table",
                id="streams_datatable",
                # id=table["id"],
                row_selectable="single",
                selected_rows=[0],
                sort_action="none",
            ),
            width=12,  # Full-width for each table
            className="mb-4",  # Add margin-bottom for spacing
        )
    )

    # Combine all elements into a single tab
    return dbc.Tab(
        dbc.Container(content, fluid=True, className="py-4"),
        label=subcategory,
        tab_id=plot_id,
    )


# @tim: FIXME: can these I/O identifiers be dynamic?
@app.callback(
    Output("updateable-line-chart", "figure"),
    [Input("datatable", "selected_rows")],
    # Output("updateable-line-chart", "figure"),
    # Input("roomclimate_table", "selected_rows"),
    # Input("streams_table", "selected_rows"),
    # [
    #     Input(table, "selected_rows")
    #     for table in ["roomclimate_table", "streams_table"]
    # ],  # Multiple Inputs
)
# def update_graph(*selected_rows_list):
def update_graph(selected_rows):
    # print(f"{ctx.triggered_id=}")
    # print(f"{ctx.states=}")
    # print(f"{ctx.triggered=}")
    # print(f"{ctx.inputs=}")
    # print("selected_rows_list", selected_rows_list)
    if selected_rows:  # Check if a row is selected
        row_idx = selected_rows[0]  # Get the index of the selected row
    else:
        row_idx = 0  # Default to the first row if nothing is selected

    # @tim: FIXME: can we deduce this from the callback context?
    # if
    config = plot_configs["RoomClimate_RoomClimate"]["TableAndTimeseries"][
        "timeseries"
    ][row_idx]
    title = config.get("title", "Line Chart")
    df = config["dataframe"]

    # @tim: FIXME: can we generalise this?
    x_data = df["Date"]
    y_data = df["Air_Temperature_Sensor"]

    # @tim: FIXME: can we utilise the create_line_plot function?
    fig = px.line(
        data_frame=df,
        x="Date",
        y=[
            "Air_Temperature_Sensor",
            "Room_Air_Temperature_Setpoint",
            "Outside_Air_Temperature_Sensor",
        ],
        title=title,
    )

    # Modify layout to place the legend below the plot
    fig.update_layout(
        font=dict(
            color="black"  # Set all plot text (title, axis labels, legend) to black
        ),
        title=dict(
            text=title,
            xanchor="center",  # Center the title
            x=0.5,  # Position the title in the horizontal center
            font=dict(color="black"),  # Set title font colour to black
        ),
        legend=dict(
            orientation="h",  # Horizontal orientation for the legend
            yanchor="bottom",  # Align the legend to the bottom
            y=-0.3,  # Push the legend below the plot
            xanchor="center",  # Centre the legend horizontally
            x=0.5,  # Centre position for the legend
        ),
    )
    return fig


# @tim: FIXME: can these I/O identifiers be dynamic?
@app.callback(
    Output("streams-updateable-line-chart", "figure"),
    [Input("streams_datatable", "selected_rows")],
)
def update_other_graph(selected_rows):
    if selected_rows:  # Check if a row is selected
        row_idx = selected_rows[0]  # Get the index of the selected row
    else:
        row_idx = 0  # Default to the first row if nothing is selected

    # @tim: FIXME: can we deduce this from the callback context?
    # if
    config = plot_configs["Streams_Streams"]["TimeseriesAndTable"]["timeseries"][
        row_idx
    ]
    title = config.get("title", "Line Chart")
    df = config["dataframe"]

    # @tim: FIXME: can we utilise the create_line_plot function?
    fig = px.line(
        data_frame=df,
        x="Date",
        y=df.columns,
        title=title,
    )

    # Modify layout to place the legend below the plot
    fig.update_layout(
        font=dict(
            color="black"  # Set all plot text (title, axis labels, legend) to black
        ),
        title=dict(
            text=title,
            xanchor="center",  # Center the title
            x=0.5,  # Position the title in the horizontal center
            font=dict(color="black"),  # Set title font colour to black
        ),
        legend=dict(
            orientation="h",  # Horizontal orientation for the legend
            yanchor="bottom",  # Align the legend to the bottom
            y=-0.3,  # Push the legend below the plot
            xanchor="center",  # Centre the legend horizontally
            x=0.5,  # Centre position for the legend
        ),
    )
    return fig


# -----------------------------  SUNBURST CHART  ----------------------------- #


# Create sunburst function
def create_sunburst_chart(
    data,
    building_column,
    parent_column,
    entity_column,
    value_column,
    title,
    color_scale="Viridis",
    height=700,
    width=700,
):
    fig = px.sunburst(
        data,
        path=[building_column, parent_column, entity_column],
        values=value_column,
        color=value_column,
        color_continuous_scale=color_scale,
        title=title,
        height=height,
        width=width,
    )
    fig.update_layout(
        title={"text": title, "x": 0.5, "xanchor": "center"},
        font_color="black",
        plot_bgcolor="white",
        coloraxis_colorbar=dict(
            title=value_column,
            orientation="h",
            yanchor="top",
            y=-0.2,
            xanchor="center",
            x=0.5,
        ),
    )
    return fig


# Function to dynamically create UI specifically for Sunburst plot types
def create_ui_for_sunburst(plot_type, plot_id):
    if plot_type == "SunburstChart":
        return html.Div(
            [
                html.Label("Select Color Scale"),
                dcc.Dropdown(
                    id={
                        "type": "sunburst-color-scale-dropdown",
                        "index": plot_id,
                    },  # 'sunburst-color-scale-dropdown' matches callback
                    options=[
                        {"label": scale, "value": scale}
                        for scale in px.colors.named_colorscales()
                    ],
                    value="Viridis",
                    clearable=False,
                ),
            ]
        )
    return None


# Callback to update SunburstChart figures based on selected color scale
@app.callback(
    Output({"type": "sunburst-graph", "index": MATCH}, "figure"),
    Input({"type": "sunburst-color-scale-dropdown", "index": MATCH}, "value"),
    State({"type": "sunburst-color-scale-dropdown", "index": MATCH}, "id"),
    prevent_initial_call=True,
)
def update_sunburst(selected_color_scale, input_id):
    plot_id = input_id["index"]  # Extract the unique plot identifier
    category_key, plot_type = plot_id.rsplit(
        "-", 1
    )  # Split to get dataframe key and plot type
    data = dataframes[category_key]  # Retrieve the relevant dataframe
    plot_settings = plot_configs[category_key][plot_type]  # Get plot settings

    updated_figure = create_sunburst_chart(
        data=data,
        building_column=plot_settings["BuildingID"],
        parent_column=plot_settings["ParentID"],
        entity_column=plot_settings["EntityID"],
        value_column=plot_settings["z-axis"],
        title=plot_settings["title"],
        color_scale=selected_color_scale,
    )

    return updated_figure


# ------------------------------  SURFACE PLOT  ------------------------------ #


# Function to create surface plot
def create_surface_plot(X, Y, Z, color_scale, title, x_label, y_label, z_label):
    # Ensure Z has the correct shape
    if len(Z) == len(X):  # If Z only has daily values
        Z = np.repeat(Z, len(Y))  # Repeat each day's value for 24 hours

    # Reshape Z to match the grid
    Z_grid = np.array(Z).reshape(len(X), len(Y))

    # Create meshgrid for X and Y
    X_grid, Y_grid = np.meshgrid(X, Y)

    fig = go.Figure(
        data=[
            go.Surface(
                x=X_grid,
                y=Y_grid,
                z=Z_grid,
                colorscale=color_scale,
                colorbar=dict(
                    orientation="h",
                    yanchor="top",
                    y=-0.3,
                    xanchor="center",
                    x=0.5,
                    title_side="bottom",
                ),
            )
        ]
    )

    fig.update_layout(
        title={"text": title, "x": 0.5, "xanchor": "center"},
        autosize=True,
        margin=dict(l=65, r=50, b=65, t=90),
        font_color="black",
        plot_bgcolor="white",
        scene=dict(
            xaxis=dict(
                title=x_label,
                mirror=True,
                ticks="outside",
                showline=True,
                linecolor="black",
                gridcolor="lightgrey",
            ),
            yaxis=dict(
                title=y_label,
                mirror=True,
                ticks="outside",
                showline=True,
                linecolor="black",
                gridcolor="lightgrey",
            ),
            zaxis=dict(
                title=z_label,
                mirror=True,
                ticks="outside",
                showline=True,
                linecolor="black",
                gridcolor="lightgrey",
            ),
        ),
    )
    return fig


# Function to dynamically create UI specifically for Surface plot types
def create_ui_for_surface_plot(plot_type, plot_id):
    if plot_type == "SurfacePlot":
        return html.Div(
            [
                html.Label("Select Color Scale"),
                dcc.Dropdown(
                    id={
                        "type": "surface-color-scale-dropdown",
                        "index": plot_id,
                    },  # 'surface-color-scale-dropdown' matches callback
                    options=[
                        {"label": scale, "value": scale}
                        for scale in px.colors.named_colorscales()
                    ],
                    value="Viridis",
                    clearable=False,
                ),
            ]
        )
    return None


# Callback to update SurfacePlot figures based on selected color scale
@app.callback(
    Output({"type": "surface-graph", "index": MATCH}, "figure"),
    Input({"type": "surface-color-scale-dropdown", "index": MATCH}, "value"),
    State({"type": "surface-color-scale-dropdown", "index": MATCH}, "id"),
    prevent_initial_call=True,
)
def update_surface_plot(selected_color_scale, input_id):
    plot_id = input_id["index"]  # Extract the unique plot identifier
    category_key, plot_type = plot_id.rsplit(
        "-", 1
    )  # Split to get dataframe key and plot type
    data = dataframes[category_key]  # Retrieve the relevant dataframe
    plot_settings = plot_configs[category_key][plot_type]  # Get plot settings

    # Extract X, Y, Z values based on plot settings
    X = data[plot_settings["X-value"]]
    Y = data[plot_settings["Y-values"][0]]  # Assuming single Y-axis value
    Z = data[plot_settings["Z-value"]]

    # Create an updated surface plot with the new color scale
    updated_figure = create_surface_plot(
        X=X,
        Y=Y,
        Z=Z,
        color_scale=selected_color_scale,
        title=plot_settings["title"],
        x_label=plot_settings["x-axis_label"],
        y_label=plot_settings["y-axis_label"],
        z_label=plot_settings["z-axis_label"],
    )

    return updated_figure


# ---------------------------------  TABLE  ---------------------------------- #


# Create table function
def create_table(
    data,
    columns,
    title,
    id="table",
    row_selectable=False,
    selected_rows=None,
    sort_action="native",
    sort_mode="multi",
):
    # table = dbc.Table.from_dataframe(
    #     data[columns],
    #     bordered=True,
    #     hover=True,
    #     responsive=True,
    #     striped=True,
    # )

    # return html.Div(
    #     [html.H5(title), table],
    # )

    # # @tim: FIXME: would rather not limit table height this way, and if it has to
    # # be done, it should be done in the CSS and ideally with a way to make the
    # # table heading sticky
    # return html.Div(
    #     [html.H5(title), table],
    #     style={"max-height": "400px", "overflow": "auto"},
    # )

    if selected_rows is None:
        selected_rows = []

    # native dash: supports exporting, sorting, and filtering, etc.
    table = dash_table.DataTable(
        id=id,
        # columns=columns,
        data=data.to_dict("records"),
        fixed_rows={"headers": True},
        export_format="csv",
        row_selectable=row_selectable,
        selected_rows=selected_rows,
        sort_action=sort_action,
        sort_mode=sort_mode,
        # filter_action="native",
        style_cell={
            "fontSize": 14,
            "textAlign": "left",
            "padding": "5px",
            # "overflow": "hidden",
            # "textOverflow": "ellipsis",
            "maxWidth": 0,
            # "width": "1px",
            # "white-space": "nowrap",
        },
        style_header={
            "fontWeight": "bold",
            "backgroundColor": "#3c9639",
            "color": "white",
        },
        style_data_conditional=[
            {
                "if": {"row_index": "odd"},
                # "backgroundColor": "rgb(220, 220, 220)",
                # "backgroundColor": "#a9dea7",
                "backgroundColor": "#ddf2dc",
            }
        ],
        style_table={
            "height": 400,
            "overflowX": "auto",
            # "table-layout": "auto"
        },
        tooltip_data=[
            {
                column: {"value": str(value), "type": "markdown"}
                for column, value in row.items()
            }
            for row in data.to_dict("records")
        ],
        tooltip_duration=None,
    )

    return html.Div(
        [html.H5(title), table],
        # style={"height": 400, "overflowY": "scroll"},
    )


################################################################################
#                                     MAIN                                     #
################################################################################

# Run the app
if __name__ == "__main__":
    # # Parse command-line arguments
    # parser = argparse.ArgumentParser()
    # parser.add_argument("data", help="Path to the data file")
    # parser.add_argument("mapper", help="Path to the mapper file")
    # parser.add_argument("model", help="Path to the model file")
    # parser.add_argument(
    #     "schema", help="Path to the schema file", nargs="?", default=None
    # )
    # parser.add_argument("-d", "--debug", help="Enable debug mode", action="store_true")
    # args = parser.parse_args()

    # # Load the data
    # db = dbmgr.DBManager(args.data, args.mapper, args.model, args.schema)

    # # Load the analytics manager
    # am = analyticsmgr.AnalyticsManager(db)
    # plot_configs = am.run_analytics()

    # construct_layout()
    # app.run_server(port=8050, debug=args.debug)

    # Hard-coded version for convenience during development
    DATA = "../datasets/bts_site_b_train/train.zip"
    MAPPER = "../datasets/bts_site_b_train/mapper_TrainOnly.csv"
    MODEL = "../datasets/bts_site_b_train/Site_B.ttl"
    SCHEMA = "../datasets/bts_site_b_train/Brick_v1.2.1.ttl"
    DEBUG = True

    # Load the data
    db = dbmgr.DBManager(DATA, MAPPER, MODEL, SCHEMA)

    # Load the analytics manager
    am = analyticsmgr.AnalyticsManager(db)
    plot_configs = sample_plot_configs
    plot_configs |= am.run_analytics()

    # ws = WeatherSensitivity(db)
    # sample_plot_configs = ws.get_weather_sensitivity_data()
    construct_layout()
    app.run_server(port=8050, debug=DEBUG)

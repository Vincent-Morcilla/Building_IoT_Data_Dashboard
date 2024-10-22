import pandas as pd
import numpy as np

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

plot_configs = {
    ("DataQuality", "DataQuality"): {
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
    ("DataQuality", "GeneralAnalysis"): {
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
    ("DataQuality", "UsageAnalysis"): {
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
    ("BuildingStructure", "BuildingStructure"): {
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
    ("BuildingStructure", "DataQuality"): {
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
    ("ModelQuality", "ClassInconsistency"): {
        "PieChartAndTable": {
            "title": "Data Sources with Inconsistent Brick Class between Model and Mapper",
            "pie_charts": [
                {
                    "title": "Proportion of Consistent vs Inconsistent Classes",
                    "labels": "brick_class_is_consistent",
                    "textinfo": "percent+label",
                    "filter": None,
                    "dataframe": dataframes["ModelQuality_ClassInconsistency"],
                },
                {
                    "title": "Inconsistent Brick Classes by Class",
                    "labels": "brick_class",
                    "values": "count",
                    "textinfo": "percent+label",
                    "filter": "brick_class_is_consistent == False",
                    "dataframe": dataframes["ModelQuality_ClassInconsistency"],
                },
            ],
            "tables": [
                {
                    "title": "Data Sources with Inconsistent Brick Class",
                    "columns": [
                        "Brick Class in Model",
                        "Brick Class in Mapper",
                        "Entity ID",
                    ],
                    "data_source": "ModelQuality_ClassInconsistency",
                    "filter": "brick_class_is_consistent == False",
                    "rows": ["brick_class", "brick_class_in_mapping", "entity"],
                    "dataframe": dataframes["ModelQuality_ClassInconsistency"],
                }
            ],
        }
    },
    ("ModelQuality", "MissingTimeseries"): {
        "PieChartAndTable": {
            "title": "Data Sources in Building Model without Timeseries Data",
            "pie_charts": [
                {
                    "title": "Proportion of Data Sources with Timeseries Data",
                    "labels": "has_data",
                    "textinfo": "percent+label",
                    "filter": None,
                    "dataframe": dataframes["ModelQuality_MissingTimeseries"],
                },
                {
                    "title": "Missing Timeseries Data by Class",
                    "labels": "brick_class",
                    "textinfo": "percent+label",
                    "filter": "has_data == False",
                    "dataframe": dataframes["ModelQuality_MissingTimeseries"],
                },
            ],
            "tables": [
                {
                    "title": "Data Sources with Missing Timeseries Data",
                    "columns": ["Brick Class", "Stream ID"],
                    "data_source": "ModelQuality_MissingTimeseries",
                    "filter": "has_data == False",
                    "rows": ["brick_class", "stream_id"],
                    "dataframe": dataframes["ModelQuality_ClassInconsistency"],
                },
                {
                    "title": "Data Sources with Available Timeseries Data",
                    "columns": ["Brick Class", "Stream ID"],
                    "data_source": "ModelQuality_MissingTimeseries",
                    "filter": "has_data == True",
                    "rows": ["brick_class", "stream_id"],
                    "dataframe": dataframes["ModelQuality_MissingTimeseries"],
                },
            ],
        }
    },
    ("ModelQuality", "RecognisedEntities"): {
        "PieChartAndTable": {
            "title": "Brick Entities in Building Model Recognised by Brick Schema",
            "pie_charts": [
                {
                    "title": "Proportion of Recognised vs Unrecognised Entities",
                    "labels": "class_in_provided_brick",
                    "textinfo": "percent+label",
                    "filter": None,
                    "dataframe": dataframes["ModelQuality_RecognisedEntities"],
                },
                {
                    "title": "Unrecognised Entities by Class",
                    "labels": "brick_class",
                    "textinfo": "percent+label",
                    "filter": "class_in_provided_brick == False",
                    "dataframe": dataframes["ModelQuality_RecognisedEntities"],
                },
            ],
            "tables": [
                {
                    "title": "Unrecognised Entities",
                    "columns": ["Brick Class", "Entity ID"],
                    "data_source": "ModelQuality_RecognisedEntities",  # Main dataframe
                    "filter": "class_in_provided_brick == False",
                    "rows": ["brick_class", "entity_id"],
                    "dataframe": dataframes["ModelQuality_RecognisedEntities"],
                },
                {
                    "title": "Recognised Entities",
                    "columns": ["Brick Class", "Entity ID"],
                    "data_source": "ModelQuality_RecognisedEntities",
                    "filter": "class_in_provided_brick == True",
                    "rows": ["brick_class", "entity_id"],
                    "dataframe": dataframes["ModelQuality_RecognisedEntities"],
                },
            ],
        }
    },
    ("RoomClimate", "Rooms"): {
        "TableAndTimeseries": {
            "title": "Room Climate",
            "table": {
                "title": "List of Rooms with Air Temperature Sensors and Setpoints",
                "columns": [
                    "Room Class",
                    "Room ID",
                ],
                "rows": ["room_class", "room_id"],
                "filter": None,
                "dataframe": pd.DataFrame(
                    {
                        "room_class": [
                            "Conference Room",
                            "Conference Room",
                            "Library",
                            "Office",
                        ],
                        "room_id": ["Room 1", "Room 2", "Room 3", "Room 4"],
                    }
                ),
            },
            "timeseries": [
                {
                    "title": "Conference Room",
                    "dataframe": pd.DataFrame(
                        {
                            "Date": pd.date_range(
                                start="2021-01-01", periods=365, freq="D"
                            ),
                            "Air_Temperature_Sensor": np.random.normal(15, 3, 365),
                            "Room_Air_Temperature_Setpoint": np.random.normal(
                                15, 1, 365
                            ),
                            "Outside_Air_Temperature_Sensor": np.random.normal(
                                15, 5, 365
                            ),
                        }
                    ),
                },
                {
                    "title": "Conference Room",
                    "dataframe": pd.DataFrame(
                        {
                            "Date": pd.date_range(
                                start="2021-01-01", periods=365, freq="D"
                            ),
                            "Air_Temperature_Sensor": np.random.normal(15, 3, 365),
                            "Room_Air_Temperature_Setpoint": np.random.normal(
                                15, 1, 365
                            ),
                            "Outside_Air_Temperature_Sensor": np.random.normal(
                                15, 5, 365
                            ),
                        }
                    ),
                },
                {
                    "title": "Library",
                    "dataframe": pd.DataFrame(
                        {
                            "Date": pd.date_range(
                                start="2021-01-01", periods=365, freq="D"
                            ),
                            "Air_Temperature_Sensor": np.random.normal(15, 3, 365),
                            "Room_Air_Temperature_Setpoint": np.random.normal(
                                15, 1, 365
                            ),
                            "Outside_Air_Temperature_Sensor": np.random.normal(
                                15, 5, 365
                            ),
                        }
                    ),
                },
                {
                    "title": "Office",
                    "dataframe": pd.DataFrame(
                        {
                            "Date": pd.date_range(
                                start="2021-01-01", periods=365, freq="D"
                            ),
                            "Air_Temperature_Sensor": np.random.normal(15, 3, 365),
                            "Room_Air_Temperature_Setpoint": np.random.normal(
                                15, 1, 365
                            ),
                            "Outside_Air_Temperature_Sensor": np.random.normal(
                                15, 5, 365
                            ),
                        }
                    ),
                },
            ],
        }
    },
    ("RoomClimate", "WeatherSensitivity"): {
        "SurfacePlot": {
            "title": "Temperature vs Weather Sensitivity",
            "X-value": "Day",
            "Y-values": ["Hour"],
            "Z-value": "WeatherSensitivity",
            "x-axis_label": "Day",
            "y-axis_label": "Hour of the Day",
            "z-axis_label": "Weather Sensitivity",
            "dataframe": dataframes["RoomClimate_WeatherSensitivity"],
        }
    },
    ("Consumption", "DataQuality"): {
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
    ("Consumption", "GeneralAnalysis"): {
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
    ("Consumption", "UsageAnalysis"): {
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
    ("NoSubcategory"): {
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
}
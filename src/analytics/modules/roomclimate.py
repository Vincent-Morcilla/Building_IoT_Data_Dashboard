"""
This module identifies rooms in the building model that have an associated 
air temperature sensor and air temperature setpoint, and for each, extracts 
the timeseries data.  If the building also has an outside air temperature 
sensor, the timeseries data for that sensor is also extracted.  The module
returns a plot configuration to display a selectable table of rooms and 
a line plot for the selected room.
"""

import pandas as pd

from analytics.dbmgr import DBManager  # only imported for type hinting
from models.types import PlotConfig  # only imported for type hinting

# Category and subcategory for the plot configuration
# The category is used to group the analysis in the app sidebar
# The subcategory is used as a tab within the category
CATEGORY = "RoomClimate"
SUBCATEGORY = "RoomClimate"


def _get_rooms_with_temp(db: DBManager) -> pd.DataFrame:
    """
    Get rooms with air temperature sensors and setpoints, and the stream ID
    of those data sources.
    """

    query = """
    SELECT ?room_id ?room_class ?ats ?ats_stream ?atsp ?atsp_stream WHERE  {
                ?ats    a                 brick:Air_Temperature_Sensor .
                ?ats    brick:isPointOf   ?room_id .
                ?ats    senaps:stream_id  ?ats_stream .
                ?atsp   a                 brick:Room_Air_Temperature_Setpoint .
                ?atsp   brick:isPointOf   ?room_id .
                ?atsp   senaps:stream_id  ?atsp_stream .
                ?room_id    a                 ?room_class   .
                ?room_class  rdfs:subClassOf   brick:Room .
            }
            ORDER BY ?room_class ?room_id
    """

    return db.query(query, graph="expanded_model", return_df=True, defrag=True)


def _get_outside_air_temp(db: DBManager) -> pd.DataFrame:
    """
    Get the outside air temperature sensor of a weather station, and the stream
    ID of that data source.
    """

    query = """
        SELECT ?oats ?oats_stream WHERE  {
            ?oats  a                 brick:Outside_Air_Temperature_Sensor .
            ?oats  brick:isPointOf   ?loc .
            ?loc   a                brick:Weather_Station .
            ?oats  senaps:stream_id  ?oats_stream .
        }
    """

    return db.query(query, graph="expanded_model", return_df=True, defrag=True)


def _build_components(df: pd.DataFrame, room_id: str, title: str) -> list:
    """
    For a given room, build the plot configuration components for the timeseries
    plot.
    """

    components = [
        {
            "type": "plot",
            "library": "px",
            "function": "line",
            "id": f"roomclimate-line-plot-{room_id}",
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
                        "text": f"Room ID: {room_id}",
                        "font": {"size": 12},
                    },
                },
                "font_color": "black",
                "plot_bgcolor": "white",
                "height": 600,
                "autosize": True,
                "margin": {
                    "t": 100,
                },
                "legend": {
                    "orientation": "h",
                    "yanchor": "top",
                    "y": -0.15,
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
        },
    ]

    return components


def _build_plot_config(table: pd.DataFrame, timeseries_data_dict: dict) -> PlotConfig:
    """
    Build the plot configuration for the room climate analysis.
    """

    plot_config = {
        (CATEGORY, SUBCATEGORY): {
            "components": [
                # Placeholder Div for timeseries plot
                {
                    "type": "placeholder",
                    "id": "roomclimate-timeseries-placeholder",
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
                    "id": "roomclimate-datatable",
                    "kwargs": {
                        "columns": [
                            {"id": "room_class", "name": "Room Class"},
                            {
                                "id": "room_id",
                                "name": "Room ID",
                            },
                        ],
                        "data": table.to_dict("records"),
                        "filter_action": "native" if len(table) > 20 else "none",
                        "fixed_rows": {"headers": True},
                        "row_selectable": "single",
                        "selected_rows": [0],  # Default to first row
                        "sort_action": "native",
                        "style_data_conditional": [
                            {
                                "if": {"row_index": "odd"},
                                "backgroundColor": "#ddf2dc",
                            }
                        ],
                        "style_table": {
                            "height": 1000,
                            "overflowX": "auto",
                        },
                        "tooltip_data": [
                            {
                                column: {"value": str(value), "type": "markdown"}
                                for column, value in row.items()
                            }
                            for row in table.to_dict("records")
                        ],
                        "tooltip_duration": None,
                    },
                },
            ],
            "interactions": [
                {
                    # Update the timeseries plot based on the selected row in the table
                    "triggers": [
                        {
                            "component_id": "roomclimate-datatable",
                            "component_property": "selected_rows",
                            "input_key": "selected_rows",
                        },
                    ],
                    "outputs": [
                        {
                            "component_id": "roomclimate-timeseries-placeholder",
                            "component_property": "children",
                        },
                    ],
                    "action": "update_components_based_on_table_selection",
                    "data_source": {
                        "table_data": table,
                        "data_dict": timeseries_data_dict,
                        "include_data_dict_in_download": True,
                    },
                    "index_column": "room_id",
                },
            ],
        },
    }

    return plot_config


def run(db: DBManager) -> PlotConfig:
    """
    Run the room climate analysis.

    Parameters
    ----------
    db : DBManager
        The database manager.

    Returns
    -------
    PlotConfig
        A plot configuration dictionary containing the analysis results.
    """

    # Get rooms with air temperature sensors and setpoints
    df = _get_rooms_with_temp(db)

    if df.empty:
        return {}

    # Get the outside air temperature sensor
    outside_air_temp = _get_outside_air_temp(db)

    # Associate the outside air temperature sensor with each room
    if not outside_air_temp.empty:
        df = df.merge(outside_air_temp, how="cross")

    # The selectable table will contain the room class and room ID
    table = df[["room_class", "room_id"]]

    timeseries_data_dict = {}

    # Iterative over each room, building a timeseries plot for each
    for _, row in df.iterrows():
        # Get the air temperature sensor stream, and resample to hourly data
        # taking the mean temperature
        ats_stream = db.get_stream(row["ats_stream"])
        ats_stream = ats_stream.pivot(
            index="time", columns="brick_class", values="value"
        )
        ats_stream = ats_stream.resample("1h").mean()

        # Get the air temperature setpoint stream, and resample to hourly data
        # taking the mean temperature
        atsp_stream = db.get_stream(row["atsp_stream"])
        atsp_stream = atsp_stream.pivot(
            index="time", columns="brick_class", values="value"
        )
        atsp_stream = atsp_stream.resample("1h").mean()

        # Combine the air temperature sensor and setpoint streams
        room_df = pd.concat([ats_stream, atsp_stream], axis=1)

        # If there is an outside air temperature sensor, get that stream,
        # resample to hourly data taking the mean temperature and add it to
        # the room dataframe
        if "oats_stream" in row:
            oats_stream = db.get_stream(row["oats_stream"])
            oats_stream = oats_stream.pivot(
                index="time", columns="brick_class", values="value"
            )
            oats_stream = oats_stream.resample("1h").mean()

            room_df = pd.concat([room_df, oats_stream], axis=1)

        # Convert the timestamp index to its own Date column
        room_df["Date"] = room_df.index

        # Replace underscores in the room class with spaces for the plot title
        room_type = row["room_class"].replace("_", " ")

        room_id = row["room_id"]
        title = f"{room_type} Mean Temperature"

        # Build the plot components for the room
        timeseries_data_dict[room_id] = _build_components(room_df, room_id, title)

    # Build and return the plot configuration for the analysis
    return _build_plot_config(table, timeseries_data_dict)

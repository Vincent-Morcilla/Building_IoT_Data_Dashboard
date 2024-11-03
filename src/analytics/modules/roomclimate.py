"""
This module identifies rooms in the building model that have an associated 
air temperature sensor and air temperature setpoint, and for each, extracts 
the timeseries data.  If the building also has an outside air temperature 
sensor, the timeseries data for that sensor is also extracted.  The module
returns a dictionary containing the analysis results.

@tim: TODO:
    - consider error conditions (e.g. missing stream data)
    - incorporate units (if available)
    - write tests
    - fix front-end code to be less hacky
"""

import pandas as pd

from analytics.dbmgr import DBManager  # only imported for type hinting


def _get_rooms_with_temp(db: DBManager) -> pd.DataFrame:
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
    # Components for this room_id
    components = [
        {
            "type": "plot",
            "library": "px",
            "function": "line",
            "id": f"roomclimate-line-plot-{room_id}",
            "kwargs": {
                "data_frame": df,
                # "height": 600,
                "x": "Date",
                # "y": "Air_Temperature_Sensor",
                "y": df.columns,
                # "y": [
                #     "Air_Temperature_Sensor",
                #     "Room_Air_Temperature_Setpoint",
                #     "Outside_Air_Temperature_Sensor",
                # ],
                # "title": title,
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
                "legend": {
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
        },
    ]

    return components


def _build_plot_config(table: pd.DataFrame, timeseries_data_dict: dict) -> dict:
    plot_config = {
        ("RoomClimate", "RoomClimate"): {
            # "title": "Room Climate",
            "components": [
                # Placeholder Div for timeseries plots
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
                    # "title": "List of Rooms with Air Temperature Sensors and Setpoints",
                    # "title_element": "H5",
                    "kwargs": {
                        "columns": [
                            {"id": "room_class", "name": "Room Class"},
                            {
                                "id": "room_id",
                                "name": "Room ID",
                            },
                        ],
                        "data": table.to_dict("records"),
                        # "filter_action": "native",
                        "fixed_rows": {"headers": True},
                        "row_selectable": "single",
                        "selected_rows": [0],  # Default to first row
                        "sort_action": "native",
                        # @tim: FIXME: this should be handled automatically by CSS
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
                        # @tim: FIXME: this should be handled automatically by CSS
                        "style_header": {
                            "fontWeight": "bold",
                            "backgroundColor": "#3c9639",
                            "color": "white",
                        },
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
                        "data_dict": timeseries_data_dict,  # Pass the dictionary of components
                    },
                    "index_column": "room_id",  # The column used as index as it's known in the dataframe not as it's known in the datatable
                },
            ],
        },
    }

    # If there are more than 10 rooms, enable filtering
    if len(table) > 20:
        plot_config[("RoomClimate", "RoomClimate")]["components"][1]["kwargs"][
            "filter_action"
        ] = "native"

    return plot_config


def run(db: DBManager) -> dict:
    """
    Run the room climate analysis.

    Parameters
    ----------
    db : DBManager
        The database manager.

    Returns
    -------
    dict
        A dictionary containing the analysis results.
    """

    df = _get_rooms_with_temp(db)

    if df.empty:
        return {}

    outside_air_temp = _get_outside_air_temp(db)

    if not outside_air_temp.empty:
        df = df.merge(outside_air_temp, how="cross")

    table = df[["room_class", "room_id"]]

    timeseries_data_dict = {}

    for _, row in df.iterrows():
        ats_stream = db.get_stream(row["ats_stream"])
        ats_stream = ats_stream.pivot(
            index="time", columns="brick_class", values="value"
        )
        ats_stream = ats_stream.resample("1h").mean()

        atsp_stream = db.get_stream(row["atsp_stream"])
        atsp_stream = atsp_stream.pivot(
            index="time", columns="brick_class", values="value"
        )
        atsp_stream = atsp_stream.resample("1h").mean()

        room_df = pd.concat([ats_stream, atsp_stream], axis=1)

        if "oats_stream" in row:
            oats_stream = db.get_stream(row["oats_stream"])
            oats_stream = oats_stream.pivot(
                index="time", columns="brick_class", values="value"
            )
            oats_stream = oats_stream.resample("1h").mean()

            room_df = pd.concat([room_df, oats_stream], axis=1)

        room_df["Date"] = room_df.index
        room_type = row["room_class"].replace("_", " ")

        room_id = row["room_id"]
        # title = f"{room_type} Mean Temperature"
        title = f"{room_type} Mean Temperature<br><sup>Room ID: {room_id}</sup>"

        timeseries_data_dict[room_id] = _build_components(room_df, room_id, title)

    return _build_plot_config(table, timeseries_data_dict)


if __name__ == "__main__":
    DATA = "../../datasets/bts_site_b_train/train.zip"
    MAPPER = "../../datasets/bts_site_b_train/mapper_TrainOnly.csv"
    MODEL = "../../datasets/bts_site_b_train/Site_B.ttl"
    SCHEMA = "../../datasets/bts_site_b_train/Brick_v1.2.1.ttl"

    import sys

    sys.path.append("..")

    db = DBManager(DATA, MAPPER, MODEL, SCHEMA)

    config = run(db)
    # print(config)

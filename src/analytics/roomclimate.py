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

from dbmgr import DBManager  # only imported for type hinting


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

    config = {
        "RoomClimate_RoomClimate": {
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
                    "dataframe": table,
                },
                "timeseries": [],
            }
        }
    }

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

        config["RoomClimate_RoomClimate"]["TableAndTimeseries"]["timeseries"].append(
            {
                "title": f"{room_type} Mean Temperature",
                "dataframe": room_df,
            }
        )

    return config


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

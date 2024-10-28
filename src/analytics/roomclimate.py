import pandas as pd


def sparql_to_df(g, q, initBindings=None):
    res = g.query(q, initBindings=initBindings)
    df = pd.DataFrame(res.bindings)
    # are these necessary?
    df.columns = df.columns.map(str)
    # df = df.map(str)
    df.drop_duplicates(inplace=True)
    return df


def get_room_temp_stream_ids(room_uri):
    room_temp_query = """
        SELECT ?ats_sid ?atsp_sid WHERE  {
            ?ats  a                 brick:Air_Temperature_Sensor .
            ?ats  brick:isPointOf   ?room_uri .
            ?ats  senaps:stream_id  ?ats_sid .
            ?atsp a                 brick:Room_Air_Temperature_Setpoint .
            ?atsp brick:isPointOf   ?room_uri .
            ?atsp senaps:stream_id  ?atsp_sid .
        }
    """

    # results = g.query(room_temp_query, initBindings={'room_uri': room_uri})
    # return results.bindings[0]
    return sparql_to_df(g, room_temp_query, initBindings={"room_uri": room_uri})


def get_rooms_with_temp(db):
    # rooms_with_temp_query = """
    #     SELECT DISTINCT ?loc_class ?loc_id ?ats_class ?ats_id ?atsp_class ?atsp_id WHERE  {
    #         ?ats_id      a                 brick:Air_Temperature_Sensor .
    #         ?ats_id      a                 ?ats_class .
    #         ?atsp_id     a                 brick:Room_Air_Temperature_Setpoint .
    #         ?atsp_id     a                 ?atsp_class .
    #         ?ats_id      brick:isPointOf   ?loc_id .
    #         ?atsp_id     brick:isPointOf   ?loc_id .
    #         ?loc_id      a                 brick:Room .
    #         ?loc_id      a                 ?loc_class   .
    #         ?loc_class   rdfs:subClassOf   brick:Room .
    #     }
    #     ORDER BY ?loc_class ?loc_id
    # """

    # return db.query(rooms_with_temp_query, graph="expanded_model", return_df=True)

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


def get_outside_air_temp(db):
    query = """
        SELECT ?oats ?oats_stream WHERE  {
            ?oats  a                 brick:Outside_Air_Temperature_Sensor .
            ?oats  brick:isPointOf   ?loc .
            ?loc   a                brick:Weather_Station .
            ?oats  senaps:stream_id  ?oats_stream .
        }
    """

    # results = g.query(outside_temp_query)
    # return results.bindings[0]
    return db.query(query, graph="expanded_model", return_df=True, defrag=True)


# def run(db: DBManager) -> dict:
def run(db) -> dict:
    """
    Run the room climate analyses.

    Parameters
    ----------
    db : DBManager
        The database manager.

    Returns
    -------
    dict
        A dictionary containing the analysis results.
    """

    # g = db.model + db.schema
    # print(f"graph contains {len(g)} triples")
    # g.expand(profile="rdfs")  # inference using RDFS reasoning
    # print(f"Expanded building model has {len(g)} triples")

    rooms_with_temp = get_rooms_with_temp(db)
    outside_air_temp = get_outside_air_temp(db)
    # print(rooms_with_temp)
    # print(outside_air_temp)
    df = rooms_with_temp.merge(outside_air_temp, how="cross")
    # print(df)
    # print(df.columns)
    # df.to_csv("roomclimate.csv")
    table = df[["room_class", "room_id"]]
    # print(table)

    config = {
        "RoomClimate_Rooms": {
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

    for index, row in df.iterrows():
        # room_uri = row["room_id"]
        # room_class = row["room"]
        # print(index)
        # print(row)
        # print()
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

        oats_stream = db.get_stream(row["oats_stream"])
        oats_stream = oats_stream.pivot(
            index="time", columns="brick_class", values="value"
        )
        oats_stream = oats_stream.resample("1h").mean()

        df = pd.concat([ats_stream, atsp_stream, oats_stream], axis=1)
        df["Date"] = df.index

        room_type = row["room_class"].replace("_", " ")

        config["RoomClimate_Rooms"]["TableAndTimeseries"]["timeseries"].append(
            {
                "title": f"{room_type} Mean Temperature",
                "dataframe": df,
            }
        )

    # print(ats_stream.head())
    # print(df.head())

    return config


if __name__ == "__main__":
    DATA = "../../datasets/bts_site_b_train/train.zip"
    MAPPER = "../../datasets/bts_site_b_train/mapper_TrainOnly.csv"
    MODEL = "../../datasets/bts_site_b_train/Site_B.ttl"
    SCHEMA = "../../datasets/bts_site_b_train/Brick_v1.2.1.ttl"

    import sys

    sys.path.append("..")

    from dbmgr import DBManager

    db = DBManager(DATA, MAPPER, MODEL, SCHEMA)

    config = run(db)
    # print(config)

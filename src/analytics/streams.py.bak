"""
This module provides a timeseries plot of each data stream in the database.
"""

from dbmgr import DBManager  # only imported for type hinting


def run(db: DBManager) -> dict:
    """
    Collect all the timeseries data.

    Parameters
    ----------
    db : DBManager
        The database manager.

    Returns
    -------
    dict
        A dictionary containing the analysis results.
    """

    df = db.mapper[["strBrickLabel", "StreamID"]].copy()
    df.rename(
        columns={"strBrickLabel": "brick_class", "StreamID": "stream_id"}, inplace=True
    )
    df.sort_values(by=["brick_class", "stream_id"], inplace=True)

    config = {
        "Streams_Streams": {
            "TimeseriesAndTable": {
                "title": "Timeseries Data Stream",
                "table": {
                    "title": "List of Timeseries Data Streams",
                    "columns": [
                        "Brick Class",
                        "Stream ID",
                    ],
                    "rows": ["brick_class", "stream_id"],
                    "filter": None,
                    "dataframe": df,
                    # "id": "streams_table",
                },
                "timeseries": [],
            }
        }
    }

    for _, row in df.iterrows():
        stream_df = db.get_stream(row["stream_id"])
        stream_df = stream_df.pivot(index="time", columns="brick_class", values="value")
        stream_df = stream_df.resample("1h").mean()

        stream_df["Date"] = stream_df.index
        # stream_df.index.names = ["Date"]

        stream_type = row["brick_class"].replace("_", " ")

        config["Streams_Streams"]["TimeseriesAndTable"]["timeseries"].append(
            {
                "title": f"{stream_type} Timeseries Data",
                "dataframe": stream_df,
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
    from dbmgr import DBManager

    db = DBManager(DATA, MAPPER, MODEL, SCHEMA)

    config = run(db)
    print(config)

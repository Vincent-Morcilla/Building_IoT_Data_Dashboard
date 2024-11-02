"""
This module performs various analyses on a Brick building model to assess its 
quality.  In particular:

- Identifies entities in the model that are not recognised by the Brick schema.
- Determines whether streams in the model have associated units.
- Checks if streams in the model have timeseries data.
- Identifies class inconsistencies between the building model and the timeseries 
  data.
"""

import numpy as np
import pandas as pd

from analytics.dbmgr import DBManager  # only imported for type hinting


def _build_master_df(db: DBManager) -> pd.DataFrame:
    """
    Get all entities in the Brick model and their associated classes, streams, and units.

    Parameters
    ----------
    db : DBManager
        The database manager.

    Returns
    -------
    pd.DataFrame
        A DataFrame containing all entities in the Brick model.
    """
    # Get all entities in the Brick model and their associated classes, streams, and units
    query = """
        SELECT ?entity_id ?brick_class ?stream_id ?named_unit ?anonymous_unit WHERE {
            ?entity_id a ?brick_class .
            OPTIONAL { ?entity_id senaps:stream_id ?stream_id } .
            OPTIONAL { ?entity_id brick:hasUnit ?named_unit .
                        filter ( strstarts(str(?named_unit),str(unit:)) ) } .
            OPTIONAL { ?entity_id brick:hasUnit [ brick:value ?anonymous_unit ] } .
            filter ( strstarts(str(?brick_class),str(brick:)) ) .
        }
    """
    df = db.query(query, return_df=True)

    # ------------------------  RECOGNISED ENTITIES  ------------------------- #

    # Check if all entities in the provided model are in the Brick schema
    df["class_in_brick_schema"] = df["brick_class"].apply(
        lambda x: ("Recognised" if (x, None, None) in db.schema else "Unrecognised")
    )

    # --------------------------  ASSOCIATED UNITS  -------------------------- #

    # If there are no named units or no anonymous units, then these columns
    # will not be present in the DataFrame. Add them if they are missing.
    if "named_unit" not in df.columns:
        df["named_unit"] = None
    if "anonymous_unit" not in df.columns:
        df["anonymous_unit"] = None

    # Add a `unit` column to the DataFrame that combines the named and anonymous units
    df = df.assign(unit=lambda x: x["named_unit"].combine_first(x["anonymous_unit"]))

    def unit_is_named(r):
        if pd.isna(r.unit):
            return None

        return not pd.isna(r.named_unit)

    df["unit_is_named"] = df.apply(unit_is_named, axis=1)

    # --------------------------  TIMESERIES DATA  --------------------------- #

    def stream_exists_in_mapping(s, mapping_df):
        if pd.isna(s):
            return None
        return str(s).strip() in mapping_df["StreamID"].values

    df["stream_exists_in_mapping"] = df["stream_id"].apply(
        stream_exists_in_mapping, args=(db.mapper,)
    )

    # -------------------------  CLASS CONSISTENCY  -------------------------- #

    # Create a temporary column with 'stream_id' converted to a string for the join
    # pylint: disable=W0108
    df["stream_id_str"] = df["stream_id"].apply(lambda x: str(x))  # noqa

    # Perform the left join
    df = pd.merge(
        df,
        db.mapper[["StreamID", "strBrickLabel"]],
        how="left",
        left_on="stream_id_str",
        right_on="StreamID",
    )

    # Rename the 'strBrickLabel' column to 'brick_class_in_mapper'
    df.rename(columns={"strBrickLabel": "brick_class_in_mapper"}, inplace=True)

    # Create a temporary column with the fragment of the 'brick_class' for comparison
    df["brick_class_fragment"] = df["brick_class"].apply(
        lambda x: str(x.fragment) if x is not None else None
    )

    # Compare the fragment of the 'brick_class' with the 'brick_class_in_mapper'
    df["brick_class_is_consistent"] = np.where(
        pd.isna(df["brick_class_in_mapper"]),  # Check if brick_class_in_mapper is empty
        None,  # Leave empty where there's no mapping value
        df["brick_class_fragment"]
        == df["brick_class_in_mapper"],  # Compare fragment with the mapping
    )

    # Drop the temporary columns
    df.drop(columns=["stream_id_str", "StreamID", "brick_class_fragment"], inplace=True)

    # -----------------------------  CLEANUP  -------------------------------- #

    for col in df.columns:
        df[col] = df[col].apply(db.defrag_uri)

    return df


def _recognised_entity_analysis(master_df):
    """
    Generate the configuration for the recognised entity analysis.

    Args:
        master_df (pd.Dataframe): The master dataframe containing the entities
            in the building model.

    Returns:
        dict: The configuration for the recognised entity analysis.
    """
    df = master_df[["brick_class", "entity_id", "class_in_brick_schema"]].copy()
    df.sort_values(
        by=["class_in_brick_schema", "brick_class", "entity_id"], inplace=True
    )

    recognised_df = df[df["class_in_brick_schema"] == "Recognised"].copy()
    recognised_df.drop(columns=["class_in_brick_schema"], inplace=True)

    unrecognised_df = df[df["class_in_brick_schema"] == "Unrecognised"].copy()
    unrecognised_df.drop(columns=["class_in_brick_schema"], inplace=True)

    # Pie Charts and Tables for Model Quality - Recognised Entities
    plot_config = {
        ("ModelQuality", "RecognisedEntities"): {
            "title": "Brick Entities in Building Model Recognised by Brick Schema",
            "components": [
                # Pie Chart: Recognised vs Unrecognised Entities
                {
                    "type": "plot",
                    "library": "go",
                    "function": "Figure",
                    "id": "model-quality-recognised-entities-pie",
                    "data_frame": df,
                    "trace_type": "Pie",
                    "data_mappings": {
                        "labels": "class_in_brick_schema",
                    },
                    "kwargs": {
                        "textinfo": "percent+label",
                        "textposition": "inside",
                        "showlegend": False,
                    },
                    "layout_kwargs": {
                        "title": {
                            "text": "Recognised vs Unrecognised Entities",
                            "font_color": "black",
                            "x": 0.5,
                            "xanchor": "center",
                        },
                    },
                    "css": {
                        "width": "50%",
                        "display": "inline-block",
                        "padding": "10px",
                    },
                },
                # Pie Chart: Unrecognised Entities by Class
                {
                    "type": "plot",
                    "library": "go",
                    "function": "Figure",
                    "id": "model-quality-recognised-entities-unrecognised-pie",
                    "data_frame": unrecognised_df,
                    "trace_type": "Pie",
                    "data_mappings": {
                        "labels": "brick_class",
                    },
                    "kwargs": {
                        "textinfo": "percent+label",
                        "textposition": "inside",
                        "showlegend": False,
                    },
                    "layout_kwargs": {
                        "title": {
                            "text": "Unrecognised Entities by Class",
                            "font_color": "black",
                            "x": 0.5,
                            "xanchor": "center",
                        },
                    },
                    "css": {
                        "width": "50%",
                        "display": "inline-block",
                        "padding": "10px",
                    },
                },
            ],
        }
    }

    if len(unrecognised_df) > 0:
        plot_config[("ModelQuality", "RecognisedEntities")]["components"].extend(
            [
                {
                    "type": "table",
                    "dataframe": unrecognised_df,
                    "id": "model-quality-unrecognised-entities-table",
                    "title": "Unrecognised Entities",
                    "title_element": "H5",
                    "kwargs": {
                        "columns": [
                            {"name": "Brick Class", "id": "brick_class"},
                            {"name": "Entity ID", "id": "entity_id"},
                        ],
                        "export_format": "csv",
                        "fixed_rows": {"headers": True},
                        "sort_action": "native",
                        "sort_mode": "multi",
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
                            for row in unrecognised_df.to_dict("records")
                        ],
                        "tooltip_duration": None,
                    },
                },
            ]
        )

    if len(recognised_df) > 0:
        plot_config[("ModelQuality", "RecognisedEntities")]["components"].extend(
            [
                {
                    "type": "table",
                    "dataframe": recognised_df,
                    "id": "model-quality-recognised-entities-table",
                    "title": "Recognised Entities",
                    "title_element": "H5",
                    "title_kwargs": {  # Adjust the styling as needed
                        "style": {
                            "margin-top": 30,
                        },
                    },
                    "kwargs": {
                        "columns": [
                            {"name": "Brick Class", "id": "brick_class"},
                            {"name": "Entity ID", "id": "entity_id"},
                        ],
                        "export_format": "csv",
                        "fixed_rows": {"headers": True},
                        "sort_action": "native",
                        "sort_mode": "multi",
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
                            for row in recognised_df.to_dict("records")
                        ],
                        "tooltip_duration": None,
                    },
                },
            ]
        )

    return plot_config


def _associated_units_analysis(master_df):
    """
    Generate the configuration for the associated units analysis.

    Args:
        master_df (pd.Dataframe): The master dataframe containing the entities
            in the building model.

    Returns:
        dict: The configuration for the associated units analysis.
    """
    df = master_df[["brick_class", "stream_id", "unit", "unit_is_named"]].copy()
    df.dropna(subset=["stream_id"], inplace=True)
    df.sort_values(by=["brick_class", "stream_id"], inplace=True)
    df["has_unit"] = df["unit"].apply(lambda x: "No units" if pd.isna(x) else "Units")

    streams_without_units = df[pd.isna(df["unit"])].copy()
    streams_without_units.sort_values(by=["brick_class", "stream_id"], inplace=True)
    streams_without_units.drop(
        columns=["unit", "unit_is_named", "has_unit"], inplace=True
    )

    stream_with_named_units = df.dropna(subset=["unit"]).copy()
    stream_with_named_units["has_named_unit"] = df["unit_is_named"].apply(
        lambda x: "Machine readable" if x else "Not machine readable"
    )

    # pylint: disable=C0121
    streams_with_anonymous_units = df[df["unit_is_named"] == False].copy()
    streams_with_anonymous_units.drop(
        columns=["unit_is_named", "has_unit"], inplace=True
    )

    plot_config = {
        ("ModelQuality", "AssociatedUnits"): {
            "title": "Brick Entities in Building Model Recognised by Brick Schema",
            "components": [
                # Pie Chart: Has vs Missing Units
                {
                    "type": "plot",
                    "library": "go",
                    "function": "Figure",
                    "id": "model-quality-associated-units-pie",
                    "data_frame": df,
                    "trace_type": "Pie",
                    "data_mappings": {
                        "labels": "has_unit",
                    },
                    "kwargs": {
                        "textinfo": "percent+label",
                        "textposition": "inside",
                        "showlegend": False,
                    },
                    "layout_kwargs": {
                        "title": {
                            "text": "Proportion of Streams with Units",
                            "font_color": "black",
                            "x": 0.5,
                            "xanchor": "center",
                        },
                    },
                    "css": {
                        "width": "50%",
                        "display": "inline-block",
                        "padding": "10px",
                    },
                },
                # Pie Chart: Machines vs Non-Machine Readable Units
                {
                    "type": "plot",
                    "library": "go",
                    "function": "Figure",
                    "id": "model-quality-associated-units-readable-pie",
                    "data_frame": stream_with_named_units,
                    "trace_type": "Pie",
                    "data_mappings": {
                        "labels": "has_named_unit",
                    },
                    "kwargs": {
                        "textinfo": "percent+label",
                        "textposition": "inside",
                        "showlegend": False,
                    },
                    "layout_kwargs": {
                        "title": {
                            "text": "Units that are Machine Readable",
                            "font_color": "black",
                            "x": 0.5,
                            "xanchor": "center",
                        },
                    },
                    "css": {
                        "width": "50%",
                        "display": "inline-block",
                        "padding": "10px",
                    },
                },
            ],
        }
    }

    if len(streams_without_units) > 0:
        plot_config[("ModelQuality", "AssociatedUnits")]["components"].extend(
            [
                # Table: Details of Streams without Units
                {
                    "type": "table",
                    "dataframe": streams_without_units,
                    "id": "model-quality-associated-units-missing-table",
                    "title": "Streams without Units",
                    "title_element": "H5",
                    "kwargs": {
                        "columns": [
                            {"name": "Brick Class", "id": "brick_class"},
                            {"name": "Stream ID", "id": "stream_id"},
                        ],
                        "export_format": "csv",
                        "fixed_rows": {"headers": True},
                        "sort_action": "native",
                        "sort_mode": "multi",
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
                            for row in streams_without_units.to_dict("records")
                        ],
                        "tooltip_duration": None,
                    },
                },
            ]
        )

    if len(streams_with_anonymous_units) > 0:
        plot_config[("ModelQuality", "AssociatedUnits")]["components"].extend(
            [
                # Table: Details of Streams with Anonymous Units
                {
                    "type": "table",
                    "dataframe": streams_with_anonymous_units,
                    "id": "model-quality-associated-units-blank-table",
                    "title": "Streams with Non-Machine Readable Units",
                    "title_element": "H5",
                    "title_kwargs": {  # Adjust the styling as needed
                        "style": {
                            "margin-top": 30,
                        },
                    },
                    "kwargs": {
                        "columns": [
                            {"name": "Brick Class", "id": "brick_class"},
                            {"name": "Stream ID", "id": "stream_id"},
                        ],
                        "export_format": "csv",
                        "fixed_rows": {"headers": True},
                        "sort_action": "native",
                        "sort_mode": "multi",
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
                            for row in streams_with_anonymous_units.to_dict("records")
                        ],
                        "tooltip_duration": None,
                    },
                },
            ]
        )

    return plot_config


def _associated_timeseries_data_analysis(master_df):
    """
    Generate the configuration for the associated timeseries data analysis.

    Args:
        master_df (pd.Dataframe): The master dataframe containing the entities
            in the building model.

    Returns:
        dict: The configuration for the associated timeseries data analysis.
    """
    df = master_df[["brick_class", "stream_id", "stream_exists_in_mapping"]].copy()
    df.dropna(subset=["stream_id"], inplace=True)
    df.sort_values(by=["brick_class", "stream_id"], inplace=True)
    df["has_data"] = df["stream_exists_in_mapping"].apply(
        lambda x: "Data" if x else "No data"
    )

    # pylint: disable=C0121
    missing_streams_by_class_pie = df[df["stream_exists_in_mapping"] == False].copy()

    have_data_df = df[df["stream_exists_in_mapping"] == True].copy()[
        ["brick_class", "stream_id"]
    ]
    missing_data_df = df[df["stream_exists_in_mapping"] == False].copy()[
        ["brick_class", "stream_id"]
    ]

    plot_config = {
        ("ModelQuality", "TimeseriesData"): {
            "title": "Data Sources in Building Model with Timeseries Data",
            "components": [
                # Pie Chart: Proportion of Data Sources with Timeseries Data
                {
                    "type": "plot",
                    "library": "go",
                    "function": "Figure",
                    "id": "model-quality-timeseries-data-pie",
                    "data_frame": df,
                    "trace_type": "Pie",
                    "data_mappings": {
                        "labels": "has_data",
                    },
                    "kwargs": {
                        "textinfo": "percent+label",
                        "textposition": "inside",
                        "showlegend": False,
                    },
                    "layout_kwargs": {
                        "title": {
                            "text": "Proportion of Data Sources with Timeseries Data",
                            "font_color": "black",
                            "x": 0.5,
                            "xanchor": "center",
                        },
                    },
                    "css": {
                        "width": "50%",
                        "display": "inline-block",
                        "padding": "10px",
                    },
                },
                # Pie Chart: Missing Data by Class
                {
                    "type": "plot",
                    "library": "go",
                    "function": "Figure",
                    "id": "model-quality-timeseries-data-missing-pie",
                    "data_frame": missing_streams_by_class_pie,
                    "trace_type": "Pie",
                    "data_mappings": {
                        "labels": "brick_class",
                    },
                    "kwargs": {
                        "textinfo": "percent+label",
                        "textposition": "inside",
                        "showlegend": False,
                    },
                    "layout_kwargs": {
                        "title": {
                            "text": "Missing Data by Class",
                            "font_color": "black",
                            "x": 0.5,
                            "xanchor": "center",
                        },
                    },
                    "css": {
                        "width": "50%",
                        "display": "inline-block",
                        "padding": "10px",
                    },
                },
            ],
        }
    }

    if len(missing_data_df) > 0:
        plot_config[("ModelQuality", "TimeseriesData")]["components"].extend(
            [
                # Table: Details of Streams without Units
                {
                    "type": "table",
                    "dataframe": missing_data_df,
                    "id": "model-quality-timeseries-data-missing-table",
                    "title": "Data Sources with Missing Timeseries Data",
                    "title_element": "H5",
                    "kwargs": {
                        "columns": [
                            {"name": "Brick Class", "id": "brick_class"},
                            {"name": "Stream ID", "id": "stream_id"},
                        ],
                        "export_format": "csv",
                        "fixed_rows": {"headers": True},
                        "sort_action": "native",
                        "sort_mode": "multi",
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
                            for row in missing_data_df.to_dict("records")
                        ],
                        "tooltip_duration": None,
                    },
                },
            ]
        )

    if len(have_data_df) > 0:
        plot_config[("ModelQuality", "TimeseriesData")]["components"].extend(
            [
                # Table: Details of Streams with Anonymous Units
                {
                    "type": "table",
                    "dataframe": have_data_df,
                    "id": "model-quality-timeseries-data-available-table",
                    "title": "Data Sources with Available Timeseries Data",
                    "title_element": "H5",
                    "title_kwargs": {  # Adjust the styling as needed
                        "style": {
                            "margin-top": 30,
                        },
                    },
                    "kwargs": {
                        "columns": [
                            {"name": "Brick Class", "id": "brick_class"},
                            {"name": "Stream ID", "id": "stream_id"},
                        ],
                        "fixed_rows": {"headers": True},
                        "export_format": "csv",
                        "sort_action": "native",
                        "sort_mode": "multi",
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
                            for row in have_data_df.to_dict("records")
                        ],
                        "tooltip_duration": None,
                    },
                },
            ]
        )

    return plot_config


def _class_consistency_analysis(master_df):
    """
    Generate the configuration for the class consistency analysis.

    Args:
        master_df (pd.Dataframe): The master dataframe containing the entities
            in the building model.

    Returns:
        dict: The configuration for the class consistency analysis.
    """
    df = master_df[
        [
            "brick_class",
            "brick_class_in_mapper",
            "entity_id",
            "brick_class_is_consistent",
        ]
    ].copy()
    df.dropna(subset=["brick_class_in_mapper"], inplace=True)
    df.sort_values(
        by=["brick_class", "brick_class_in_mapper", "entity_id"], inplace=True
    )
    df["consistency"] = df["brick_class_is_consistent"].apply(
        lambda x: "Consistent" if x else "Inconsistent"
    )

    # pylint: disable=C0121
    inconsistent_df = df[df["brick_class_is_consistent"] == False].copy()[
        ["brick_class", "brick_class_in_mapper", "entity_id"]
    ]

    # Pie Charts and Tables for Model Quality - Class Consistency
    plot_config = {
        ("ModelQuality", "ClassConsistency"): {
            "title": "Data Sources in Building Model with Timeseries Data",
            "components": [
                # Pie Chart: Consistent vs Inconsistent Classes
                {
                    "type": "plot",
                    "library": "go",
                    "function": "Figure",
                    "id": "model-quality-class-consistency-pie",
                    "data_frame": df,
                    "trace_type": "Pie",
                    "data_mappings": {
                        "labels": "consistency",
                    },
                    "kwargs": {
                        "textinfo": "percent+label",
                        "textposition": "inside",
                        "showlegend": False,
                    },
                    "layout_kwargs": {
                        "title": {
                            "text": "Proportion of Data Sources",
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
                # Pie Chart: Inconsistent Classes by Brick Class
                {
                    "type": "plot",
                    "library": "go",
                    "function": "Figure",
                    "id": "model-quality-inconsistent-classes-pie",
                    "data_frame": inconsistent_df,
                    "trace_type": "Pie",
                    "data_mappings": {
                        "labels": "brick_class",
                    },
                    "kwargs": {
                        "textinfo": "percent+label",
                        "textposition": "inside",
                        "showlegend": False,
                    },
                    "layout_kwargs": {
                        "title": {
                            "text": "Inconsistent Data Sources by Class",
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
            ],
        }
    }

    if len(inconsistent_df) > 0:
        plot_config[("ModelQuality", "ClassConsistency")]["components"].extend(
            [
                # Table: Details of Inconsistent Classes
                {
                    "type": "table",
                    "dataframe": inconsistent_df,
                    "id": "model-quality-inconsistent-classes-table",
                    "title": "Data Sources with Inconsistent Brick Class",
                    "title_element": "H5",
                    "kwargs": {
                        "columns": [
                            {"name": "Brick Class in Model", "id": "brick_class"},
                            {
                                "name": "Brick Class in Mapper",
                                "id": "brick_class_in_mapper",
                            },
                            {"name": "Entity ID", "id": "entity_id"},
                        ],
                        "export_format": "csv",
                        "fixed_rows": {"headers": True},
                        "sort_action": "native",
                        "sort_mode": "multi",
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
                            for row in inconsistent_df.to_dict("records")
                        ],
                        "tooltip_duration": None,
                    },
                },
            ]
        )

    return plot_config


def run(db: DBManager) -> dict:
    """
    Run the model quality analyses.

    Parameters
    ----------
    db : DBManager
        The database manager.

    Returns
    -------
    dict
        A dictionary containing the analysis results.
    """
    df = _build_master_df(db)

    analyses = {}
    analyses |= _recognised_entity_analysis(df)
    analyses |= _associated_units_analysis(df)
    analyses |= _associated_timeseries_data_analysis(df)
    analyses |= _class_consistency_analysis(df)

    return analyses

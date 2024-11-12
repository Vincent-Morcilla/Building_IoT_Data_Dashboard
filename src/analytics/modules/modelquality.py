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
from models.types import PlotConfig  # only imported for type hinting
from models.types import PlotComponentConfig  # only imported for type hinting
from models.types import TableComponentConfig  # only imported for type hinting

# Category for the plot configuration
# The category is used to group the analysis in the app sidebar
CATEGORY = "ModelQuality"


################################################################################
#                               HELPER FUNCTIONS                               #
################################################################################


def _generate_id(
    category: str, subcategory: str, component: str, suffix: str | None = None
) -> str:
    """
    Generate a unique ID for a component in the plot configuration.

    Parameters
    ----------
    category : str
        The category of the analysis.
    subcategory : str
        The subcategory of the analysis.
    component : str
        The component of the analysis.
    suffix : str, optional
        A suffix to append to the ID, by default None

    Returns
    -------
    str
        The generated ID.
    """

    def _clean_string(s):
        if not isinstance(s, str):
            return s
        return s.lower().replace(" ", "-")

    category = _clean_string(category)
    subcategory = _clean_string(subcategory)
    component = _clean_string(component)

    if suffix is None:
        return f"{category}-{subcategory}-{component}"

    suffix = _clean_string(suffix)
    return f"{category}-{subcategory}-{component}-{suffix}"


def _generate_green_scale_colour_map(labels: list) -> dict:
    """
    Generate a colour map for the labels based on a green scale.

    Parameters
    ----------
    labels : list
        The unique labels to generate colours for.

    Returns
    -------
    dict
        A dictionary mapping labels to colours.
    """

    n_colors = len(labels)

    if n_colors == 1:
        return {labels[0]: "#3c9639"}  # theme green
    if n_colors == 2:
        return {labels[0]: "#3c9639", labels[1]: "#2d722c"}

    # Create evenly spaced values between 0 and 1
    values = np.linspace(0, 1, n_colors)

    # Define our green scale endpoints
    dark_green = np.array([30, 76, 29])  # #1e4c1d
    theme_green = np.array([60, 150, 57])  # #3c9639
    light_green = np.array([184, 228, 183])  # #b8e4b7

    colour_map = {}
    for label, v in zip(labels, values):
        if v <= 0.5:
            # Interpolate between dark and theme green
            rgb = dark_green + (2 * v) * (theme_green - dark_green)
        else:
            # Interpolate between theme and light green
            v = v - 0.5
            rgb = theme_green + (2 * v) * (light_green - theme_green)

        # Convert to hex
        hex_color = f"#{int(rgb[0]):02x}{int(rgb[1]):02x}{int(rgb[2]):02x}"
        colour_map[label] = hex_color

    return colour_map


def _build_pie_chart_component(
    title: str, component_id: str, df: pd.DataFrame, label: str
) -> PlotComponentConfig:
    """
    Build a pie chart component for the analysis.

    Parameters
    ----------
    title : str
        The title of the pie chart.
    component_id : str
        The unique ID of the component.
    df : pd.DataFrame
        The DataFrame containing the data for the pie chart.
    label : str
        The column in the DataFrame containing the labels for the pie chart.

    Returns
    -------
    PlotComponentConfig
        The configuration for the pie chart component.
    """

    return {
        "type": "plot",
        "library": "go",
        "function": "Figure",
        "id": component_id,
        "data_frame": df,
        "trace_type": "Pie",
        "data_mappings": {
            "labels": label,
        },
        "kwargs": {
            "textinfo": "percent+label",
            "textposition": "inside",
            "showlegend": False,
            "marker": {
                "colors": df[label].map(
                    _generate_green_scale_colour_map(df[label].unique())
                ),
            },
        },
        "layout_kwargs": {
            "title": {
                "text": title,
                "font_color": "black",
                "x": 0.5,
                "xanchor": "center",
            },
            "margin": {"t": 50, "b": 50, "l": 50, "r": 50},
        },
        "css": {
            "width": "50%",
            "display": "inline-block",
            "padding": "10px",
        },
    }


def _build_table_component(
    title: str,
    component_id: str,
    df: pd.DataFrame,
    columns: list,
    title_padding: int | None = None,
) -> TableComponentConfig:
    """
    Build a table component for the analysis.

    Parameters
    ----------
    title : str
        The title of the table.
    component_id : str
        The unique ID of the component.
    df : pd.DataFrame
        The DataFrame containing the data for the table.
    columns : list
        The columns to display in the table.
    title_padding : int, optional
        The padding for the title, by default None

    Returns
    -------
    TableComponentConfig
        The configuration for the table component.
    """

    component = {
        "type": "table",
        "dataframe": df,
        "id": component_id,
        "title": title,
        "title_element": "H5",
        "kwargs": {
            "columns": columns,
            "export_format": "csv",
            "filter_action": "native" if len(df) > 20 else "none",
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
                for row in df.to_dict("records")
            ],
            "tooltip_duration": None,
        },
    }

    if title_padding is not None:
        component["title_kwargs"] = {
            "style": {
                "margin-top": title_padding,
            },
        }

    return component


################################################################################
#                                   ANALYSIS                                   #
################################################################################


def _build_master_df(db: DBManager) -> pd.DataFrame:
    """
    Get all entities in the Brick model and their associated classes, streams,
    and units, then perform some preliminary analysis on the data to builkd a
    master DataFrame in preparation for the individual analyses.

    Parameters
    ----------
    db : DBManager
        The database manager.

    Returns
    -------
    pd.DataFrame
        A DataFrame containing all entities in the Brick model.
    """

    # Sparql query to get all entities in the Brick model and their associated
    # classes, streams, and units
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

    # Execute the query and return the results as a DataFrame
    df = db.query(query, return_df=True)

    if df.empty:
        return df

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

    # Add a column to the DataFrame to flag if the unit is named, i.e., machine-readable
    # The colunm will be True if the unit is named, False if it is anonymous, and None if
    # there is no unit
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
    df["stream_id_str"] = df["stream_id"].apply(lambda x: str(x))

    # Join the DataFrame with the mapping file to get the 'strBrickLabel' column
    df = pd.merge(
        df,
        db.mapper[["StreamID", "strBrickLabel"]],
        how="left",
        left_on="stream_id_str",
        right_on="StreamID",
    )

    # Replace NaN values with None, for consistency with the rest of the DataFrame
    df["strBrickLabel"] = df["strBrickLabel"].apply(
        lambda x: None if pd.isna(x) else x.strip()
    )

    # Rename the 'strBrickLabel' column to 'brick_class_in_mapper'
    df.rename(columns={"strBrickLabel": "brick_class_in_mapper"}, inplace=True)

    # The brick class in the building model is a BrickURI object, which is not
    # directly comparable to the 'brick_class_in_mapper' column. To compare them,
    # we will create a temporary column with the fragment of the 'brick_class' for
    # comparison
    df["brick_class_fragment"] = df["brick_class"].apply(
        lambda x: str(x.fragment) if x is not None else None
    )

    # Compare the fragment of the 'brick_class' with the 'brick_class_in_mapper'
    df["brick_class_is_consistent"] = np.where(
        pd.isna(df["brick_class_in_mapper"]),  # Check if brick_class_in_mapper is empty
        None,  # Leave empty where there's no mapping value
        df["brick_class_fragment"]
        == df["brick_class_in_mapper"],  # Otherwise compare fragment with the mapping
    )

    # Drop the temporary columns
    df.drop(columns=["stream_id_str", "StreamID", "brick_class_fragment"], inplace=True)

    # -----------------------------  CLEANUP  -------------------------------- #

    # Remove the URI prefixes from all cells in the DataFrame
    for col in df.columns:
        df[col] = df[col].apply(db.defrag_uri)

    return df


def _recognised_entity_analysis(master_df: pd.DataFrame) -> PlotConfig:
    """
    Generate the configuration for the recognised entity analysis.  A recognised
    entity is an entity in the building model that is of a class recognised by
    the Brick schema.

    The purpose of this analysis is to identify the entities in the building
    model that are not recognised by the Brick schema.

    Args:
        master_df (pd.Dataframe): The master dataframe containing the entities
            in the building model.

    Returns:
        PlotConfig: The configuration for the recognised entity analysis.
    """

    subcategory = "RecognisedEntities"
    page_title = "Brick Entities in Building Model Recognised by Brick Schema"

    # Extract and sort the relevant columns from the master DataFrame
    df = master_df[["brick_class", "entity_id", "class_in_brick_schema"]].copy()
    df.sort_values(
        by=["class_in_brick_schema", "brick_class", "entity_id"], inplace=True
    )

    # Split the DataFrame into recognised and unrecognised entities
    recognised_df = df[df["class_in_brick_schema"] == "Recognised"].copy()
    recognised_df.drop(columns=["class_in_brick_schema"], inplace=True)

    unrecognised_df = df[df["class_in_brick_schema"] == "Unrecognised"].copy()
    unrecognised_df.drop(columns=["class_in_brick_schema"], inplace=True)

    components = []

    # Configure the Recognised vs Unrecognised Entities pie chart
    pie_config_1 = _build_pie_chart_component(
        "Recognised vs Unrecognised Entities",
        _generate_id(CATEGORY, subcategory, "pie-1"),
        df,
        "class_in_brick_schema",
    )
    components.append(pie_config_1)

    # Configure the Unrecognised Entities by Class pie chart
    pie_config_2 = _build_pie_chart_component(
        "Unrecognised Entities by Class",
        _generate_id(CATEGORY, subcategory, "pie-2"),
        unrecognised_df,
        "brick_class",
    )
    components.append(pie_config_2)

    # If there are unrecognised entities, configure the Unrecognised Entities table
    if len(unrecognised_df) > 0:
        table_config = _build_table_component(
            "Unrecognised Entities",
            _generate_id(CATEGORY, subcategory, "table-1"),
            unrecognised_df,
            [
                {"name": "Brick Class", "id": "brick_class"},
                {"name": "Entity ID", "id": "entity_id"},
            ],
        )
        components.append(table_config)

    # If there are recognised entities, configure the Recognised Entities table
    if len(recognised_df) > 0:
        table_config = _build_table_component(
            "Recognised Entities",
            _generate_id(CATEGORY, subcategory, "table-2"),
            recognised_df,
            [
                {"name": "Brick Class", "id": "brick_class"},
                {"name": "Entity ID", "id": "entity_id"},
            ],
            30 if len(unrecognised_df) > 0 else None,
        )

        components.append(table_config)

    return {
        (CATEGORY, subcategory): {
            "title": page_title,
            "components": components,
        }
    }


def _associated_units_analysis(master_df: pd.DataFrame) -> PlotConfig:
    """
    Generate the configuration for the associated units analysis.

    The purpose of this analysis is to determine whether streams in the building
    model have associated units.  And of those that have units, whether the units
    are machine-readable.  A unit is considered machine-readable if it is linked
    to an ontology, such as the QUDT ontology, rather than being encoded as an
    anonymous node (aka blank node) in the model.

    Args:
        master_df (pd.Dataframe): The master dataframe containing the entities
            in the building model.

    Returns:
        PlotConfig: The configuration for the associated units analysis.
    """

    subcategory = "AssociatedUnits"
    page_title = "Brick Entities in Building Model with Associated Units"

    # Extract and sort the relevant columns and rows from the master DataFrame
    df = master_df[["brick_class", "stream_id", "unit", "unit_is_named"]].copy()
    df.dropna(subset=["stream_id"], inplace=True)
    df.sort_values(by=["brick_class", "stream_id"], inplace=True)

    # Add a column to the DataFrame to label if the stream has a unit
    df["has_unit"] = df["unit"].apply(lambda x: "No units" if pd.isna(x) else "Units")

    # Split the DataFrame into streams without units, streams with named units,
    # and streams with anonymous units
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

    components = []

    # Configure the Proportion of Streams with Units pie chart
    pie_config_1 = _build_pie_chart_component(
        "Proportion of Streams with Units",
        _generate_id(CATEGORY, subcategory, "pie-1"),
        df,
        "has_unit",
    )

    components.append(pie_config_1)

    # Configure the Proportion of Streams with Machine Readable Units pie chart
    pie_config_2 = _build_pie_chart_component(
        "Units that are Machine Readable",
        _generate_id(CATEGORY, subcategory, "pie-2"),
        stream_with_named_units,
        "has_named_unit",
    )

    components.append(pie_config_2)

    # If there are streams without units, configure the Streams without Units table
    if len(streams_without_units) > 0:
        table_config = _build_table_component(
            "Streams without Units",
            _generate_id(CATEGORY, subcategory, "table-1"),
            streams_without_units,
            [
                {"name": "Brick Class", "id": "brick_class"},
                {"name": "Stream ID", "id": "stream_id"},
            ],
        )

        components.append(table_config)

    # If there are streams with anonymous units, configure the Streams with
    # Anonymous Units table
    if len(streams_with_anonymous_units) > 0:
        table_config = _build_table_component(
            "Streams with Non-Machine Readable Units",
            _generate_id(CATEGORY, subcategory, "table-2"),
            streams_with_anonymous_units,
            [
                {"name": "Brick Class", "id": "brick_class"},
                {"name": "Stream ID", "id": "stream_id"},
            ],
            30 if len(streams_without_units) > 0 else None,
        )

        components.append(table_config)

    return {
        (CATEGORY, subcategory): {
            "title": page_title,
            "components": components,
        }
    }


def _associated_timeseries_data_analysis(master_df: pd.DataFrame) -> PlotConfig:
    """
    Generate the configuration for the associated timeseries data analysis.

    The purpose of this analysis is to determine whether streams in the building
    model have associated timeseries data.

    Args:
        master_df (pd.Dataframe): The master dataframe containing the entities
            in the building model.

    Returns:
        PlotConfig: The configuration for the associated timeseries data analysis.
    """

    subcategory = "TimeseriesData"
    page_title = "Data Sources in Building Model with Timeseries Data"

    # Extract and sort the relevant columns and rows from the master DataFrame
    df = master_df[["brick_class", "stream_id", "stream_exists_in_mapping"]].copy()
    df.dropna(subset=["stream_id"], inplace=True)
    df.sort_values(by=["brick_class", "stream_id"], inplace=True)

    # Add a column to the DataFrame to label if the stream has timeseries data
    df["has_data"] = df["stream_exists_in_mapping"].apply(
        lambda x: "Data" if x else "No data"
    )

    # Split the DataFrame into streams with and without timeseries data
    # pylint: disable=C0121
    missing_streams_by_class_pie = df[df["stream_exists_in_mapping"] == False].copy()

    have_data_df = df[df["stream_exists_in_mapping"] == True].copy()[
        ["brick_class", "stream_id"]
    ]
    missing_data_df = df[df["stream_exists_in_mapping"] == False].copy()[
        ["brick_class", "stream_id"]
    ]

    components = []

    # Configure the Proportion of Data Sources with Timeseries Data pie chart
    pie_config_1 = _build_pie_chart_component(
        "Proportion of Data Sources with Timeseries Data",
        _generate_id(CATEGORY, subcategory, "pie-1"),
        df,
        "has_data",
    )

    components.append(pie_config_1)

    # Configure the Missing Data by Class pie chart
    pie_config_2 = _build_pie_chart_component(
        "Missing Data by Class",
        _generate_id(CATEGORY, subcategory, "pie-2"),
        missing_streams_by_class_pie,
        "brick_class",
    )

    components.append(pie_config_2)

    # If there are streams without timeseries data, configure the Streams without
    # Timeseries Data table
    if len(missing_data_df) > 0:
        table_config = _build_table_component(
            "Data Sources with Missing Timeseries Data",
            _generate_id(CATEGORY, subcategory, "table-1"),
            missing_data_df,
            [
                {"name": "Brick Class", "id": "brick_class"},
                {"name": "Stream ID", "id": "stream_id"},
            ],
        )

        components.append(table_config)

    # If there are streams with timeseries data, configure the Streams with
    # Timeseries Data table
    if len(have_data_df) > 0:
        table_config = _build_table_component(
            "Data Sources with Available Timeseries Data",
            _generate_id(CATEGORY, subcategory, "table-2"),
            have_data_df,
            [
                {"name": "Brick Class", "id": "brick_class"},
                {"name": "Stream ID", "id": "stream_id"},
            ],
            30 if len(missing_data_df) > 0 else None,
        )

        components.append(table_config)

    return {
        (CATEGORY, subcategory): {
            "title": page_title,
            "components": components,
        }
    }


def _class_consistency_analysis(master_df: pd.DataFrame) -> PlotConfig:
    """
    Generate the configuration for the class consistency analysis.

    The purpose of this analysis is to identify class inconsistencies between the
    building model and the timeseries data.  Specifically, this analysis compares
    the classes of the entities in the building model with the classes of the
    entities in mapping file that links the model to the timeseries data.

    Args:
        master_df (pd.Dataframe): The master dataframe containing the entities
            in the building model.

    Returns:
        PlotConfig: The configuration for the class consistency analysis.
    """

    subcategory = "ClassConsistency"
    page_title = "Data Sources in Building Model with Timeseries Data"

    # Extract and sort the relevant columns and rows from the master DataFrame
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

    # Add a column to the DataFrame to label if the class is consistent
    df["consistency"] = df["brick_class_is_consistent"].apply(
        lambda x: "Consistent" if x else "Inconsistent"
    )

    # Extract the inconsistent entities from the DataFrame
    # pylint: disable=C0121
    inconsistent_df = df[df["brick_class_is_consistent"] == False].copy()[
        ["brick_class", "brick_class_in_mapper", "entity_id"]
    ]

    components = []

    # Configure the Proportion of Data Sources pie chart
    pie_config_1 = _build_pie_chart_component(
        "Brick Class Consistency of Data Sources",
        _generate_id(CATEGORY, subcategory, "pie-1"),
        df,
        "consistency",
    )

    components.append(pie_config_1)

    # Configure the Inconsistent Data Sources by Class pie chart
    pie_config_2 = _build_pie_chart_component(
        "Inconsistent Data Sources by Class",
        _generate_id(CATEGORY, subcategory, "pie-2"),
        inconsistent_df,
        "brick_class",
    )

    components.append(pie_config_2)

    # If there are inconsistent entities, configure the Inconsistent Data Sources
    # table
    if len(inconsistent_df) > 0:
        table_config = _build_table_component(
            "Data Sources with Inconsistent Brick Class",
            _generate_id(CATEGORY, subcategory, "table-1"),
            inconsistent_df,
            [
                {"name": "Brick Class in Model", "id": "brick_class"},
                {"name": "Brick Class in Mapper", "id": "brick_class_in_mapper"},
                {"name": "Entity ID", "id": "entity_id"},
            ],
        )

        components.append(table_config)

    return {
        (CATEGORY, subcategory): {
            "title": page_title,
            "components": components,
        }
    }


def run(db: DBManager) -> PlotConfig:
    """
    Run the model quality analyses.

    Parameters
    ----------
    db : DBManager
        The database manager.

    Returns
    -------
    PlotConfig
        A plot configuration dictionary containing the analysis results.
    """
    df = _build_master_df(db)

    if df.empty:
        return {
            (CATEGORY, "Error"): {
                "components": [
                    {
                        "type": "error",
                        "message": "Unable to identify any entities in the building model.",
                        "css": {"color": "#a93932", "font-weight": "bold"},
                    }
                ],
            }
        }

    analyses = {}
    analyses |= _recognised_entity_analysis(df)
    analyses |= _associated_units_analysis(df)
    analyses |= _associated_timeseries_data_analysis(df)
    analyses |= _class_consistency_analysis(df)

    return analyses

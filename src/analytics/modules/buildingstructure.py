"""
This module interprets and structures this hierarchy of locations, systems, 
equipment, and points of a building in order to create an interactive 
visualization of the layout of a building. This hierarchy goes from the 
general down through the levels of rooms and equipment to the systems they 
are connected with. Points will also be mapped to their respective rooms, 
equipment, or systems in order for one to have detailed visualizations of 
them.

The module returns a dictionary of configuration that could be used in a 
frontend sunburst chart to let users explore the building structure 
interactively.

"""

import pandas as pd

from analytics.dbmgr import DBManager  # only imported for type hinting


def _get_building_hierarchy(db: DBManager) -> pd.DataFrame:
    """
    This function retrieves the building hierarchy data, including various
    relationships within a building, such as buildings, systems, equipment,
    levels, rooms, HVAC, and sensors. This data is represented as a
    parent-child structure, enabling visualization of a complete building
    hierarchy.

    Args:
        db (DBManager): Database manager instance to execute SPARQL queries.

    Returns:
        pd.DataFrame: DataFrame containing the hierarchy of building
                      entities with columns for parent, parent label, child,
                      child label and entity type.
    """

    query = """
        SELECT DISTINCT ?parent ?parentLabel ?child ?childLabel ?entityType
        WHERE {
            # Define the building itself as the root
            ?building a brick:Building . 
            ?building a ?buildingLabel .
            BIND('' AS ?parent)
            BIND('' AS ?parentLabel)
            BIND(?building AS ?child)
            BIND(?buildingLabel AS ?childLabel)
            BIND('Location' AS ?entityType)

            OPTIONAL {
                # Match the root node for the building
                {
                    ?building a brick:Building .
                    ?building a ?buildingLabel .
                    BIND('Location' AS ?entityType)
                    BIND('' AS ?parent)
                    BIND('' AS ?parentLabel)
                    BIND(?building AS ?child)
                    BIND(?buildingLabel AS ?childLabel)
                    BIND('Location' AS ?entityType)
                }

                # Match System entities connected to the building
                UNION
                {
                    ?building a brick:Building . 
                    ?building a ?buildingLabel .
                    ?system brick:hasLocation ?building .
                    ?system a ?systemLabel .
                    ?systemLabel brick:hasAssociatedTag tag:System . 
                    BIND(?building AS ?parent)
                    BIND(?buildingLabel AS ?parentLabel)
                    BIND(?system AS ?child)
                    BIND(?systemLabel AS ?childLabel)
                    BIND('System' AS ?entityType)
                }

                # match equipments connected to the building
                UNION {
                    ?equip brick:hasLocation ?building .
                    ?building a ?buildingLabel .
                    ?equip a ?equipLabel .
                    ?equipLabel brick:hasAssociatedTag tag:Equipment . 
                    BIND(?building AS ?parent)
                    BIND(?buildingLabel AS ?parentLabel)
                    BIND(?equip AS ?child)
                    BIND(?equipLabel AS ?childLabel)
                    BIND('Equipment' AS ?entityType)
                }
                # Match all levels within the building
                UNION
                {
                    ?level brick:isPartOf ?building .
                    ?building a ?buildingLabel .
                    ?level a ?levelLabel .
                    BIND(?building AS ?parent)
                    BIND(?buildingLabel AS ?parentLabel)
                    BIND(?level AS ?child)
                    BIND(?levelLabel AS ?childLabel)
                    BIND('Location' AS ?entityType)
                }
                # Match rooms within each level
                UNION {
                    ?level brick:isPartOf ?building .
                    ?room brick:isPartOf ?level .
                    ?level a ?levelLabel .
                    ?room a ?roomLabel .
                    BIND(?level AS ?parent)
                    BIND(?levelLabel AS ?parentLabel)
                    BIND(?room AS ?child)
                    BIND(?roomLabel AS ?childLabel)
                    BIND('Location' AS ?entityType)
                }
                # Match HVAC connected to rooms
                UNION {
                    ?level brick:isPartOf ?building .
                    ?room brick:isPartOf ?level .
                    ?room a ?roomLabel .
                    ?hvac brick:hasPart ?room .
                    ?hvac a ?hvacLabel .
                    BIND(?room AS ?parent) 
                    BIND(?roomLabel AS ?parentLabel)
                    BIND(?hvac AS ?child)
                    BIND(?hvacLabel AS ?childLabel)
                    BIND('Location' AS ?entityType)
                }
                # Match equipments connected to a room
                UNION {
                    ?level brick:isPartOf ?building .
                    ?room brick:isPartOf ?level .
                    ?room a ?roomLabel .
                    ?equip1 brick:isFedBy | brick:hasLocation | brick:isPartOf ?room .
                    ?equip1 a ?equip1Label
                    BIND(?room AS ?parent)
                    BIND(?roomLabel AS ?parentLabel)
                    BIND(?equip1 AS ?child)
                    BIND(?equip1Label AS ?childLabel)
                    BIND('Equipment' AS ?entityType)          
                }
                # match equipment connected to another equipment
                UNION {
                    ?level brick:isPartOf ?building .
                    ?room brick:isPartOf ?level .
                    ?room a ?roomLabel .
                    ?equip1 brick:isFedBy | brick:hasLocation | brick:isPartOf ?room .
                    ?equip1 a ?equip1Label .
                    ?equip2 brick:isFedBy | brick:hasLocation | brick:isPartOf ?equip1 .
                    ?equip2 a ?equip2Label
                    BIND(?equip1 AS ?parent)
                    BIND(?equip1Label AS ?parentLabel)
                    BIND(?equip2 AS ?child)
                    BIND(?equip2Label AS ?childLabel)
                    BIND('Equipment' AS ?entityType)
                }
                # match equipment connected to a room by inverse (equip brick:feeds room)
                UNION {
                    ?level brick:isPartOf ?building .
                    ?room brick:isPartOf ?level .
                    ?room a ?roomLabel .
                    ?equip1 brick:feeds ?room .
                    ?equip1 a ?equip1Label .
                    BIND(?room AS ?parent)
                    BIND(?roomLabel AS ?parentLabel)
                    BIND(?equip1 AS ?child)
                    BIND(?equip1Label AS ?childLabel)
                    BIND('Equipment' AS ?entityType)
                }
                # match equipment connected to another equipment by inverse (equip1 brick:feeds equip2)
                UNION {
                    ?level brick:isPartOf ?building .
                    ?room brick:isPartOf ?level .
                    ?room a ?roomLabel .
                    ?equip1 brick:isFedBy | brick:hasLocation | brick:isPartOf ?room .
                    ?equip1 a ?equip1Label .
                    ?equip1 brick:feeds ?equip2 .
                    ?equip2 a ?equip2Label
                    BIND(?equip1 AS ?parent)
                    BIND(?equip1Label AS ?parentLabel)
                    BIND(?equip2 AS ?child)
                    BIND(?equip2Label AS ?childLabel)
                    BIND('Equipment' AS ?entityType)
                }

                # match sensors connected to levels
                UNION {
                    ?level brick:isPartOf ?building .
                    ?building a ?buildingLabel .
                    ?level a ?levelLabel .
                    ?sensor brick:isPointOf ?level .
                    ?sensor a ?sensorLabel .
                    BIND(?level AS ?parent)
                    BIND(?levelLabel AS ?parentLabel)
                    BIND(?sensor AS ?child)
                    BIND(?sensorLabel AS ?childLabel)
                    BIND('Point' AS ?entityType)
                }    

                # match sensors connected to rooms
                UNION {
                    ?level brick:isPartOf ?building .
                    ?room brick:isPartOf ?level .
                    ?level a ?levelLabel .
                    ?room a ?roomLabel .
                    ?sensor brick:isPointOf ?room .
                    ?sensor a ?sensorLabel .
                    BIND(?room AS ?parent)
                    BIND(?roomLabel AS ?parentLabel)
                    BIND(?sensor AS ?child)
                    BIND(?sensorLabel AS ?childLabel)
                    BIND('Point' AS ?entityType)
                }
                # sensor connected to an equipment in a room 
                UNION {
                    ?level brick:isPartOf ?building .
                    ?room brick:isPartOf ?level .
                    ?room a ?roomLabel .
                    ?equip1 brick:hasLocation ?room .
                    ?equip1 a ?equip1Label .
                    ?sensor brick:isPointOf ?equip1 .
                    ?sensor a ?sensorLabel .
                    BIND(?equip1 AS ?parent)
                    BIND(?equip1Label AS ?parentLabel)
                    BIND(?sensor AS ?child)
                    BIND(?sensorLabel AS ?childLabel)
                    BIND('Point' AS ?entityType)         
                }
                # sensor connected to equipment connected to another equipment
                UNION {
                    ?level brick:isPartOf ?building .
                    ?room brick:isPartOf ?level .
                    ?room a ?roomLabel .
                    ?equip1 brick:hasLocation ?room .
                    ?equip1 a ?equip1Label .
                    ?equip2 brick:hasLocation | brick:isPartOf ?equip1 .
                    ?equip2 a ?equip2Label .
                    ?sensor brick:isPointOf ?equip2 .
                    ?sensor a ?sensorLabel .
                    BIND(?equip2 AS ?parent)
                    BIND(?equip2Label AS ?parentLabel)
                    BIND(?sensor AS ?child)
                    BIND(?sensorLabel AS ?childLabel)
                    BIND('Point' AS ?entityType)
                }
                # sensors connetcted to equipment connected to another equipment by inverse (equip1 brick:feeds equip2)
                UNION {
                    ?level brick:isPartOf ?building .
                    ?room brick:isPartOf ?level .
                    ?room a ?roomLabel .
                    ?equip1 brick:isFedBy | brick:hasLocation | brick:isPartOf ?room .
                    ?equip1 a ?equip1Label .
                    ?equip1 brick:feeds ?equip2 .
                    ?equip2 a ?equip2Label .
                    ?sensor brick:isPointOf ?equip2 .
                    ?sensor a ?sensorLabel .
                    BIND(?equip2 AS ?parent)
                    BIND(?equip2Label AS ?parentLabel)
                    BIND(?sensor AS ?child)
                    BIND(?sensorLabel AS ?childLabel)
                    BIND('Point' AS ?entityType)
                }

                # Match sensors connected to System entities connected to building
                UNION
                {
                    ?system brick:hasLocation ?building .
                    ?building a ?buildingLabel .
                    ?system a ?systemLabel .
                    ?systemLabel brick:hasAssociatedTag tag:System .
                    ?sensor brick:isPointOf ?system .
                    ?sensor a ?sensorLabel .
                    BIND(?system AS ?parent)
                    BIND(?systemLabel AS ?parentLabel)
                    BIND(?sensor AS ?child)
                    BIND(?sensorLabel AS ?childLabel)
                    BIND('Point' AS ?entityType)    
                }
            }    
        }
    """

    # Execute the SPARQL query on the specified graph and return results as a DataFrame
    return db.query(query, graph="schema+model", return_df=True, defrag=True)


def _get_building_area(db: DBManager) -> pd.DataFrame:
    """
    This function retrieves the hierarchical area structure within a
    building, including the building itself, levels, rooms, and HVAC.
    This function organizes building areas as a parent-child
    relationship for hierarchical representation.

    Args:
        db (DBManager): Database manager instance used to execute SPARQL
        queries.

    Returns:
        pd.DataFrame: DataFrame containing area hierarchy of building
                      elements, with columns for parent, parent label,
                      child, child label, and entity type.
    """
    query = """
        SELECT DISTINCT ?parent ?parentLabel ?child ?childLabel ?entityType
        WHERE {
            # Define the building as the root area.
            ?building a brick:Building . 
            ?building a ?buildingLabel
            BIND(?building AS ?location)
            BIND(?buildingLabel AS ?locationLabel)
            BIND('Location' AS ?entityType)

            OPTIONAL {
                # Match root node: the building
                {
                    ?building a brick:Building .
                    ?building a ?buildingLabel .
                    BIND('Location' AS ?entityType)
                    BIND('' AS ?parent)
                    BIND('' AS ?parentLabel)
                    BIND(?building AS ?child)
                    BIND(?buildingLabel AS ?childLabel)
                }
                # Match all levels within the building
                UNION
                {
                    ?level brick:isPartOf ?building .
                    ?building a ?buildingLabel .
                    ?level a ?levelLabel .
                    BIND(?building AS ?parent)
                    BIND(?buildingLabel AS ?parentLabel)
                    BIND(?level AS ?child)
                    BIND(?levelLabel AS ?childLabel)
                    BIND('Location' AS ?entityType)
                }
                # Match all rooms within each level
                UNION {
                    ?level brick:isPartOf ?building .
                    ?room brick:isPartOf ?level .
                    ?level a ?levelLabel .
                    ?room a ?roomLabel .
                    BIND(?level AS ?parent)
                    BIND(?levelLabel AS ?parentLabel)
                    BIND(?room AS ?child)
                    BIND(?roomLabel AS ?childLabel)
                    BIND('Location' AS ?entityType)
                }
                # Match HVAC connected to rooms
                UNION {
                    ?level brick:isPartOf ?building .
                    ?room brick:isPartOf ?level .
                    ?room a ?roomLabel .
                    ?hvac brick:hasPart ?room .
                    ?hvac a ?hvacLabel .
                    BIND(?room AS ?parent) 
                    BIND(?roomLabel AS ?parentLabel)
                    BIND(?hvac AS ?child)
                    BIND(?hvacLabel AS ?childLabel)
                    BIND('Location' AS ?entityType)
                }
            }    
        }
    """

    # Execute the SPARQL query on the specified graph and return results as a DataFrame
    return db.query(query, graph="schema+model", return_df=True, defrag=True)


def run(db: DBManager) -> dict:
    """
    Executes the building structure analysis by generating hierarchical
    representations for building elements such as locations, systems,
    equipment, and points. It outputs configurations for visualisations.

    Parameters
    ----------
    db : DBManager
      The database manager used to execute queries on the building schema.

    Returns
    -------
    dict
        A dictionary containing configurations for the building structure
        visualizations.
    """

    # Retrieve building hierarchy data
    data_hierarchy = _get_building_hierarchy(db)

    # Check if data_hierarchy is empty; if so, return an empty dictionary
    if data_hierarchy.empty:
        return {}

    # Extract unique ID for each child entity
    data_hierarchy["ids"] = data_hierarchy["child"].str.split("#").str[-1]

    # Extract labels for child entities
    data_hierarchy["labels"] = (
        data_hierarchy["childLabel"].str.split("#").str[-1].str.replace("_", " ")
    )

    # Extract unique ID for each parent entity
    data_hierarchy["parents"] = data_hierarchy["parent"].str.split("#").str[-1]

    # Ensure entity types are in string format for color-map compatibility
    data_hierarchy["entityType"] = data_hierarchy["entityType"].apply(str)

    # Filter out required columns for hierarchy visualisation
    data_hierarchy = data_hierarchy[["ids", "labels", "parents", "entityType"]]

    # Retrieve building area data
    df_area = _get_building_area(db)

    # Check if df_area is empty; if so, return an empty dictionary
    if df_area.empty:
        return {}

    # Extract unique ID for each child entity in building area
    df_area["ids"] = df_area["child"].str.split("#").str[-1]

    # Extract labels for child entities in building area
    df_area["labels"] = (
        df_area["childLabel"].str.split("#").str[-1].str.replace("_", " ")
    )

    # Extract unique ID for each parent entity in building area
    df_area["parents"] = df_area["parent"].str.split("#").str[-1]

    # Ensure entity types are in string format for color-map compatibility
    df_area["entityType"] = df_area["entityType"].apply(str)

    # Filter out required columns for hierarchy visualisation
    data_area = df_area[["ids", "labels", "parents", "entityType"]]

    # Define color map for different entity types
    color_map = {
        # "(?)":"black",
        "Location": "LightCoral",
        "System": "Lavender",
        "Equipment": "#32BF84",
        "Point": "Gold",
    }

    # Custom annotations to create a legend in the plot layout
    annotations = []
    legend_y = 1.02  # Initial y position for the legend
    spacing = 0.03  # Vertical Spacing for each legend item

    # Loop through color map to add legend items
    for category, color in color_map.items():
        annotation = {
            "x": 0.85,
            "y": legend_y,
            "xref": "paper",
            "yref": "paper",
            "showarrow": False,
            "text": f"<span style='font-size:30px; color:{color};'>■</span> <span style='font-size:12px;'>{category}</span>",
            "font": {"size": 12},
            "xanchor": "left",
        }
        legend_y -= spacing
        annotations.append(annotation)

    # Define visualisation configurations for both building hierarchy and building locations
    config = {
        ("BuildingStructure", "BuildingHierarchy"): {
            "title": None,
            "components": [
                {
                    "type": "plot",
                    "library": "px",
                    "function": "sunburst",
                    "id": "building-hierarchy-sunburst",
                    "kwargs": {
                        "data_frame": data_hierarchy,
                        "parents": "parents",
                        "names": "labels",
                        "ids": "ids",
                        "title": None,
                        "height": 1000,
                        "color": "entityType",
                        "color_discrete_map": color_map,
                    },
                    "layout_kwargs": {
                        "font_color": "black",
                        "plot_bgcolor": "white",
                        "annotations": annotations,
                        "margin": {"t": 0, "b": 0, "l": 0, "r": 0},
                    },
                    "css": {
                        "marginLeft": "2%",
                    },
                },
            ],
        },
        ("BuildingStructure", "BuildingLocations"): {
            "title": None,
            "components": [
                {
                    "type": "plot",
                    "library": "px",
                    "function": "sunburst",
                    "id": "building-locations-sunburst",
                    "kwargs": {
                        "data_frame": data_area,
                        "parents": "parents",
                        "names": "labels",
                        "ids": "ids",
                        "title": None,
                        "height": 1000,
                        "color": "entityType",
                        "color_discrete_map": color_map,
                    },
                    "layout_kwargs": {
                        "font_color": "black",
                        "plot_bgcolor": "white",
                        "margin": {"t": 0, "b": 0, "l": 0, "r": 0},
                    },
                    "css": {
                        "marginLeft": "2%",
                    },
                },
            ],
        },
    }

    return config

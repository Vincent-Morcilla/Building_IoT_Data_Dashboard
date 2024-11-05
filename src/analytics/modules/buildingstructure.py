"""
This module interprets and structures this hierarchy of locations, systems, equipment, and points of a building 
in order to create an interactive visualization of the layout of a building. This hierarchy goes from the general 
down through the levels of rooms and equipment to the systems they are connected with. Points will also be mapped 
to their respective rooms, equipment, or systems in order for one to have detailed visualizations of them.

The module returns a dictionary of configuration that could be used in a frontend sunburst chart to let users 
explore the building structure interactively.

@bassel: TODO:
    - Handle potential issues with missing or incomplete data in the hierarchy
    - Implement comprehensive tests to validate hierarchy extraction
"""

import pandas as pd

from analytics.dbmgr import DBManager  # only imported for type hinting


def _get_building_hierarchy(db: DBManager) -> pd.DataFrame:
    query = """
        SELECT DISTINCT ?parent ?parentLabel ?child ?childLabel ?building ?buildingLabel ?system ?systemLabel ?entityType
        WHERE {
            # Define the building
            ?building a brick:Building . 
            ?building a ?buildingLabel .
            BIND('' AS ?parent)
            BIND('' AS ?parentLabel)
            BIND(?building AS ?child)
            BIND(?buildingLabel AS ?childLabel)
            BIND('Location' AS ?entityType)

            OPTIONAL {
                # Match root node: building
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

                # Match System entities connected to building
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

                # match equipments connected to building
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
                # Match all levels
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
                # Match all rooms
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
                # match all HVAC
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
                # sensors connected to equipment feeds a room
                # UNION {
                #     ?level brick:isPartOf ?building .
                #     ?room brick:isPartOf ?level .
                #     ?room a ?roomLabel .
                #     ?equip1 brick:feeds ?room .
                #     ?equip1 a ?equip1Label .
                #     ?sensor brick:isPointOf ?equip1 .
                #     ?sensor a ?sensorLabel .
                #     BIND(?equip1 AS ?parent)
                #     BIND(?equip1Label AS ?parentLabel)
                #     BIND(?sensor AS ?child)
                #     BIND(?sensorLabel AS ?childLabel)
                # }
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
    return db.query(query, graph="schema+model", return_df=True, defrag=True)


def _get_building_area(db: DBManager) -> pd.DataFrame:
    query = """
        SELECT DISTINCT ?parent ?parentLabel ?child ?childLabel ?entityType
        WHERE {
            # Define the building
            ?building a brick:Building . 
            ?building a ?buildingLabel
            BIND(?building AS ?location)
            BIND(?buildingLabel AS ?locationLabel)
            BIND('Location' AS ?entityType)

            OPTIONAL {
                # Match root node: building
                {
                    ?building a brick:Building .
                    ?building a ?buildingLabel .
                    BIND('Location' AS ?entityType)
                    BIND('' AS ?parent)
                    BIND('' AS ?parentLabel)
                    BIND(?building AS ?child)
                    BIND(?buildingLabel AS ?childLabel)
                }
                # Match all levels
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
                # Match all rooms
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
                # match all HVAC
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
    return db.query(query, graph="schema+model", return_df=True, defrag=True)


def run(db: DBManager) -> dict:
    """
    Run the building structure analysis.

    Parameters
    ----------
    db : DBManager
        The database manager.

    Returns
    -------
    dict
        A dictionary containing the analysis results.
    """

    data_hierarchy = _get_building_hierarchy(db)

    if data_hierarchy.empty:
        return {}

    data_hierarchy["ids"] = data_hierarchy["child"].str.split("#").str[-1]
    data_hierarchy["labels"] = (
        data_hierarchy["childLabel"].str.split("#").str[-1].str.replace("_", " ")
    )
    data_hierarchy["parents"] = data_hierarchy["parent"].str.split("#").str[-1]
    data_hierarchy["entityType"] = data_hierarchy["entityType"].apply(str)

    data_hierarchy = data_hierarchy[["ids", "labels", "parents", "entityType"]]

    df_area = _get_building_area(db)

    if df_area.empty:
        return {}

    df_area["ids"] = df_area["child"].str.split("#").str[-1]
    df_area["labels"] = (
        df_area["childLabel"].str.split("#").str[-1].str.replace("_", " ")
    )
    df_area["parents"] = df_area["parent"].str.split("#").str[-1]
    df_area["entityType"] = df_area["entityType"].apply(str)

    data_area = df_area[["ids", "labels", "parents", "entityType"]]

    color_map = {
        # "(?)":"black",
        "Location": "LightCoral",
        "System": "Lavender",
        "Equipment": "#32BF84",
        "Point": "Gold",
    }

    # Custom annotations for the legend
    annotations = []
    legend_y = 1.05  # Initial y position for the legend
    spacing = 0.03

    for category, color in color_map.items():
        annotation = {
            "x": 1.05,
            "y": legend_y,
            "xref": "paper",
            "yref": "paper",
            "showarrow": False,
            "text": f"<span style='font-size:30px; color:{color};'>■</span> <span style='font-size:12px;'>{category}</span>",
            "font": {"size": 12},
        }
        legend_y -= spacing
        annotations.append(annotation)

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
                        "title": "Building Hierarchy",
                        "height": 1000,
                        "width": 1000,
                        "color": "entityType",
                        "color_discrete_map": color_map,
                    },
                    "layout_kwargs": {
                        "title": {
                            "text": "Building Hierarchy",
                            "x": 0.5,
                            "xanchor": "center",
                            "font": {"size": 35},
                        },
                        "font_color": "black",
                        "plot_bgcolor": "white",
                        "coloraxis_colorbar": {
                            "title": "Level",
                            "orientation": "h",
                            "yanchor": "top",
                            "y": -0.2,
                            "xanchor": "center",
                            "x": 0.5,
                        },
                        "annotations": annotations,
                    },
                    "css": {
                        "padding": "10px",
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
                        "title": "Building Locations",
                        "height": 1000,
                        "width": 1000,
                        "color": "entityType",
                        "color_discrete_map": color_map,
                    },
                    "layout_kwargs": {
                        "title": {
                            "text": "Building Locations",
                            "x": 0.5,
                            "xanchor": "center",
                            "font": {"size": 35},
                        },
                        "font_color": "black",
                        "plot_bgcolor": "white",
                        "coloraxis_colorbar": {
                            "title": "Level",
                            "orientation": "h",
                            "yanchor": "top",
                            "y": -0.2,
                            "xanchor": "center",
                            "x": 0.5,
                        },
                        "annotations": annotations,
                    },
                    "css": {
                        "padding": "10px",
                    },
                },
            ],
        },
    }

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

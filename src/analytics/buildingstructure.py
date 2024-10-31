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


def _get_building_hierarchy(db: DBManager) -> pd.DataFrame:
    query = """
        SELECT DISTINCT ?parent ?parentLabel ?child ?childLabel ?building ?buildingLabel ?location ?locationLabel ?system ?systemLabel ?equipment ?equipmentLabel ?sensor ?sensorLabel
        WHERE {
            # Define the building
            ?building a brick:Building . 
            ?building a ?buildingLabel
            BIND(?building AS ?location)
            BIND(?buildingLabel AS ?locationLabel)

            OPTIONAL {
                # Match System entities connected to building
                {
                    ?sys brick:hasLocation ?building .
                    ?building a ?buildingLabel .
                    ?sys a ?sysLabel .
                    ?sysLabel brick:hasAssociatedTag tag:System . 
                    BIND(?building AS ?parent)
                    BIND(?buildingLabel AS ?parentLabel)
                    BIND(?sys AS ?child)
                    BIND(?sysLabel AS ?childLabel)
                    BIND(?sys AS ?system)
                    BIND(?sysLabel AS ?systemLabel)
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
                }
            }    
        }
    """
    return db.query(query, return_df=True, defrag=True)


def _get_building_area(db: DBManager) -> pd.DataFrame:
    query = """
        SELECT DISTINCT ?parent ?parentLabel ?child ?childLabel ?building ?buildingLabel ?location ?locationLabel ?system ?systemLabel ?equipment ?equipmentLabel ?sensor ?sensorLabel
        WHERE {
            # Define the building
            ?building a brick:Building . 
            ?building a ?buildingLabel
            BIND(?building AS ?location)
            BIND(?buildingLabel AS ?locationLabel)

            OPTIONAL {
                # Match all levels
                {
                    ?level brick:isPartOf ?building .
                    ?building a ?buildingLabel .
                    ?level a ?levelLabel .
                    BIND(?building AS ?parent)
                    BIND(?buildingLabel AS ?parentLabel)
                    BIND(?level AS ?child)
                    BIND(?levelLabel AS ?childLabel)
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
                }
                # # match all HVAC
                # UNION {
                #     ?level brick:isPartOf ?building .
                #     ?room brick:isPartOf ?level .
                #     ?room a ?roomLabel .
                #     ?hvac brick:hasPart ?room .
                #     ?hvac a ?hvacLabel .
                #     BIND(?room AS ?parent) 
                #     BIND(?roomLabel AS ?parentLabel)
                #     BIND(?hvac AS ?child)
                #     BIND(?hvacLabel AS ?childLabel)
                # }
            }    
        }
    """
    return db.query(query, return_df=True, defrag=True)


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

    df = _get_building_hierarchy(db)

    # print(df.head())

    if df.empty:
        return {}

    df["labels"] = (
        df["childLabel"].str.split("#").str[-1]
        + " ("
        + df["child"].str.split("#").str[-1]
        + ")"
    )
    df["parents"] = (
        df["parentLabel"].str.split("#").str[-1]
        + " ("
        + df["parent"].str.split("#").str[-1]
        + ")"
    )

    data = df[["labels", "parents"]]

    df_area = _get_building_area(db)

    if df_area.empty:
        return {}

    df_area["labels"] = (
        df_area["childLabel"].str.split("#").str[-1]
        + " ("
        + df_area["child"].str.split("#").str[-1]
        + ")"
    )
    df_area["parents"] = (
        df_area["parentLabel"].str.split("#").str[-1]
        + " ("
        + df_area["parent"].str.split("#").str[-1]
        + ")"
    )

    data_area = df_area[["labels", "parents"]]

    # df1 = pd.DataFrame(
    #     {
    #         "labels": ["Water_Tank dd2a3659_6674_4f80_88b7_be63664ff2e3	", "Basement e8e7705d_b87a_4bf3_b454_202ca27ea7ad", "Floor 6e0854f9_3888_4867_9176_0693de75f841", "Floor ef790dae_bb9b_4cd7_a52c_ea35841f02dd"],
    #         "parents": ["Building 13211186_beb4_4227_bd2d_0644e860886e", "Building 13211186_beb4_4227_bd2d_0644e860886e", "Building 13211186_beb4_4227_bd2d_0644e860886e", "Building 13211186_beb4_4227_bd2d_0644e860886e"],
    #     }
    # )

    config = {
        "BuildingStructure_BuildingHierarchy": {
            "SunburstChart": {
                "title": "Building Hierarchy Sunburst",
                "EntityID": "EntityID",
                "EntityType": "EntityType",
                "ParentID": "ParentID",
                "BuildingID": "BuildingID",
                "z-axis": "Level",
                "z-axis_label": "Hierarchy Level",
                "dataframe": data,
            }
        },
        "BuildingStructure_BuildingArea": {
            "SunburstChart": {
                "title": "Building Area Sunburst",
                "EntityID": "AreaID",
                "EntityType": "AreaType",
                "ParentID": "ParentAreaID",
                "BuildingID": "BuildingAreaID",
                "z-axis": "AreaLevel",
                "z-axis_label": "Area Level",
                "dataframe": data_area,
            }
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

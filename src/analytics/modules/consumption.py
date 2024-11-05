"""
This module identifies meters within the building model, including electrical, gas, and water meters, and extracts 
the timeseries data associated with sensors. The data are then processed to resample and interpolate missing values 
according to the meter type-instantaneous for the electrical power meter, and cumulative for an electrical energy,
gas, water meter. Units are incorporated where available and defaults are filled for missing values-cubic meters for 
gas and water meters.

The result is a configuration dictionary for front-end visualization that plots line plots showing Graph the usage 
over time in a timeline, enabling the end user to explore energy and resource usage patterns.

@bassel: TODO:
    - Handle missing data and edge cases for stream data
    - Write tests to verify data extraction and processing
"""

import pandas as pd
import numpy as np


from analytics.dbmgr import DBManager  # only imported for type hinting


def _get_building_meters(db: DBManager) -> pd.DataFrame:
    query = """
SELECT  ?equipment ?equipment_type ?sensor ?sensor_type ?unit ?stream_id
WHERE {
    # match all equipments
    ?equipment a ?equipment_type .
    ?equipment_type brick:hasAssociatedTag tag:Equipment . 

    # Ensure equipment also has tag:Meter
    ?equipment_type brick:hasAssociatedTag tag:Meter .

    {
        ?sensor a ?sensor_type .
        ?sensor brick:isPointOf ?equipment .
        ?sensor senaps:stream_id ?stream_id .    
        ?sensor brick:powerComplexity [ brick:value ?power_complexity ] .
        FILTER(?power_complexity = "real")
        OPTIONAL { ?sensor brick:hasUnit [ brick:value ?unit ] }
       
    }
    Union
    {
        ?sensor a ?sensor_type .
        ?sensor brick:isPointOf ?equipment .
        ?sensor senaps:stream_id ?stream_id .
        ?sensor_type brick:hasAssociatedTag tag:Usage .
        OPTIONAL { ?sensor brick:hasUnit [ brick:value ?unit ] }
    }
}
    """
    return db.query(query, graph='schema+model', return_df=True, defrag=True)


def run(db: DBManager) -> dict:
    """
    Run the building consumption analysis.

    Parameters
    ----------
    db : DBManager
        The database manager.

    Returns
    -------
    dict
        A dictionary containing the analysis results.
    """

    data_meters = _get_building_meters(db)

    if data_meters.empty:
        return {}
    
     # Apply the helper function to fetch sensor data for each stream_id in data_meters
    data_meters["sensor_data"] = data_meters["stream_id"].apply(db.get_stream)

    grouped_meters = data_meters.groupby(["equipment_type", "sensor_type"])

    # Initialize config dictionary
    config = {}

    # Iterate over each group and populate the config
    for (meter_type, sensor_type), group in grouped_meters:
        # Create a unique identifier for each configuration entry / frontend page tab
        config_key = ("Consumption", f"{meter_type.replace('_', '')}")

        # Prepare a dictionary to hold the combined resampled data for each timestamp
        combined_data = None

        # Process data for each sensor in the group
        for idx, row in group.iterrows():
            sensor_data = row['sensor_data']
            unit = row.get('unit', 'Unknown')
            if pd.isna(unit):
                if meter_type.lower() in ["building_gas_meter", "building_water_meter"]:
                    unit = "Cubic meters"
                else:
                    unit = None 

            if sensor_data is not None and not sensor_data.empty:
                # Pivot and resample sensor data to 1-hour intervals
                sensor_data = sensor_data.pivot(index="time", columns="brick_class", values="value")

                # Resample based on sensor type: Cumulative -> sum resampling, Instantaneous -> mean resampling
                if sensor_type.lower() in ["electrical_power_sensor"]:
                    # For electrical power sensors (Measure instantaneous consumption at a specific moment in time)
                    sensor_data = sensor_data.resample("10min").mean()
                elif sensor_type.lower() in ["usage_sensor", "electrical_energy_sensor"]:
                    # For water and gas electrical sensors (Measure cumulative consumption over time)
                    sensor_data = sensor_data.resample("10min").sum()

                sensor_data = sensor_data.interpolate(method='linear')

                # Sum sensor values across the same timestamps
                if combined_data is None:
                    combined_data = sensor_data  # Initialize combined data
                else:
                    combined_data = combined_data.add(sensor_data, fill_value=0)  # Add current sensor's values


        # Prepare the final DataFrame for plotting
        if combined_data is not None:
            plot_data = pd.DataFrame({
                "Timestamp": combined_data.index,
                "Usage": combined_data.sum(axis=1),  # Sum across all columns to get total usage per timestamp
                "Sensor": f"{meter_type} Combined ({unit})"
            })

        # Define the configuration structure for the frontend based on meter_type and sensor_type
        component = {
            "type": "plot",
            "library": "px",
            "function": "line",
            "id": f"{meter_type.lower()}-{sensor_type.lower()}-aggregated-line-plot",
            "kwargs": {
                "data_frame": plot_data,
                "x": "Timestamp",
                "y": "Usage",
                "color": "Sensor",
            },
            "layout_kwargs": {
                "title": {
                    "text": f"Aggregated {meter_type.replace('_', ' ')} - {sensor_type.replace('_', ' ')} Usage",
                    "x": 0.5,
                    "xanchor": "center",
                },
                "font_color": "black",
                "plot_bgcolor": "white",
                "font": {"size": 15},
                "legend": {
                    "orientation": "h",
                    "yanchor": "top",
                    "y": -0.3,
                    "xanchor": "center",
                    "x": 0.5,
                    "font": {"size": 15},
                },
                "xaxis": {
                    "title": "Date",
                    "mirror": True,
                    "ticks": "outside",
                    "showline": True,
                    "linecolor": "black",
                    "gridcolor": "lightgrey",
                },
                "yaxis": {
                    "title": "Total Usage",
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
        }

        # Check if config_key already exists in config
        if config_key in config:
            # If it exists, append the new component to the existing components list
            config[config_key]["components"].append(component)
        else:
            # If it does not exist, create a new entry with the component
            config[config_key] = {
                "title": f"{meter_type.replace('_', ' ')}",
                "components": [component],
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

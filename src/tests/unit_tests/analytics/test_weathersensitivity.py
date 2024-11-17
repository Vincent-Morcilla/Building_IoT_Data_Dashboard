"""All the functions for Weather Sensitivity"""

import sys
import pandas as pd
from analytics.dbmgr import DBManager

# Silence irrelevant warnings
# pylint: disable=protected-access

from analytics.modules.weathersensitivity import (
    _get_electric_energy_query_str,
    _get_electric_power_query_str,
    _get_gas_query_str,
    _get_chiller_query_str,
    _get_water_query_str,
    _get_boiler_query_str,
    _get_outside_air_temperature_query_str,
    WeatherSensitivity,
)

sys.path.append("src/analytics/modules")


def test_get_electric_energy_query_str():
    """test get_electric_energy_query_str function"""
    expected = """
            SELECT ?meter ?sensor ?stream_id ?phase_count ?phases ?unit ?power_complexity ?power_flow
            WHERE {
                ?sensor rdf:type brick:Electrical_Energy_Sensor .
                ?meter rdf:type brick:Electrical_Meter .
                ?sensor brick:isPointOf ?meter .
                ?sensor senaps:stream_id ?stream_id .
                OPTIONAL { ?sensor brick:electricalPhaseCount [ brick:value ?phase_count ] . }
                OPTIONAL { ?sensor brick:electricalPhases [ brick:value ?phases ] . }
                OPTIONAL { ?sensor brick:hasUnit [ brick:value ?unit ] . }
                OPTIONAL { ?sensor brick:powerComplexity [ brick:value ?power_complexity ] . }
                OPTIONAL { ?sensor brick:powerFlow [ brick:value ?power_flow ] . }
            }
            ORDER BY ?meter
            """

    assert expected == _get_electric_energy_query_str()


def test_get_electric_power_query_str():
    """test get_electric_power_query_str function"""
    expected = """
            SELECT ?meter ?sensor ?stream_id ?phase_count ?phases ?unit ?power_complexity ?power_flow
            WHERE {
                ?sensor rdf:type brick:Electrical_Power_Sensor .
                ?meter rdf:type brick:Electrical_Meter .
                ?sensor brick:isPointOf ?meter .
                ?sensor senaps:stream_id ?stream_id .
                OPTIONAL { ?sensor brick:electricalPhaseCount [ brick:value ?phase_count ] . }
                OPTIONAL { ?sensor brick:electricalPhases [ brick:value ?phases ] . }
                OPTIONAL { ?sensor brick:hasUnit [ brick:value ?unit ] . }
                OPTIONAL { ?sensor brick:powerComplexity [ brick:value ?power_complexity ] . }
                OPTIONAL { ?sensor brick:powerFlow [ brick:value ?power_flow ] . }
            }
            ORDER BY ?meter
            """

    assert expected == _get_electric_power_query_str()


def test_get_gas_query_str():
    """test get_gas_query_str function"""
    expected = """
            SELECT ?meter ?sensor ?stream_id
            WHERE {
                ?sensor rdf:type brick:Usage_Sensor .
                ?meter rdf:type brick:Building_Gas_Meter .
                ?sensor brick:isPointOf ?meter .
                ?sensor senaps:stream_id ?stream_id
            }
            ORDER BY ?meter
        """
    assert expected == _get_gas_query_str()


def test_get_chiller_query_str():
    """test get_chiller_query_str function"""
    expected = """
            SELECT ?meter ?sensor ?stream_id
            WHERE {
                ?sensor rdf:type brick:Chilled_Water_Differential_Temperature_Sensor .
                ?meter rdf:type brick:Chiller .
                ?sensor brick:isPointOf ?meter .
                ?sensor senaps:stream_id ?stream_id
            }
            ORDER BY ?meter
            """
    assert expected == _get_chiller_query_str()


def test_get_water_query_str():
    """test get_water_query_str function"""
    expected = """
            SELECT ?meter ?sensor ?stream_id
            WHERE {
                ?sensor rdf:type brick:Usage_Sensor .
                ?meter rdf:type brick:Building_Water_Meter .
                ?sensor brick:isPointOf ?meter .
                ?sensor senaps:stream_id ?stream_id
            }
            ORDER BY ?meter
            """
    assert expected == _get_water_query_str()


def test_get_boiler_query_str():
    """test get_boiler_query_str function"""
    expected = """
            SELECT ?meter ?sensor ?stream_id
            WHERE {
                ?sensor rdf:type brick:Water_Temperature_Sensor .
                ?meter rdf:type brick:Hot_Water_System .
                ?sensor brick:isPointOf ?meter .
                ?sensor senaps:stream_id ?stream_id
            }
            ORDER BY ?meter
            """
    assert expected == _get_boiler_query_str()


def test_get_outside_air_temperature_query_str():
    """test get_outside_air_temperature_query_str function"""

    expected = """
        SELECT ?sensor ?stream_id 
        WHERE {
            ?sensor rdf:type brick:Outside_Air_Temperature_Sensor .
            ?sensor brick:isPointOf   ?loc .
            ?loc a brick:Weather_Station .
            ?sensor senaps:stream_id ?stream_id .
        }
        ORDER BY ?stream_id
        """
    assert expected == _get_outside_air_temperature_query_str()


def test_transpose_dataframe_for_vis():
    """test transpose_dataframe_for_vis function"""
    df_input = pd.DataFrame(
        {
            "date": ["2024-01-01", "2024-01-02"],
            "sensor_1": [0.8, 0.6],
            "sensor_2": [0.5, 0.7],
        }
    )

    df_expected = pd.DataFrame(
        {
            "Date": ["2024-01-01", "2024-01-01", "2024-01-02", "2024-01-02"],
            "Sensor ID": ["sensor_1", "sensor_2", "sensor_1", "sensor_2"],
            "Correlation": [0.8, 0.5, 0.6, 0.7],
        }
    )

    df_output = WeatherSensitivity._transpose_dataframe_for_vis(df_input)
    pd.testing.assert_frame_equal(df_output, df_expected)


def test_prepare_data_for_vis():
    """test prepare_data_for_vis function"""
    df_input = pd.DataFrame(
        {
            "Date": ["2024-01-01", "2024-01-02"],
            "Sensor ID": ["sensor_1", "sensor_2"],
            "Correlation": [0.8, 0.5],
        }
    )

    meter = "electricity"
    title = "Weather Sensitivity Heatmap"

    expected_output = {
        "type": "plot",
        "library": "go",
        "function": "Heatmap",
        "id": f"ws-heatmap-{meter}",
        "data_frame": df_input,
        "trace_type": "Heatmap",
        "data_mappings": {
            "x": "Date",
            "y": "Sensor ID",
            "z": "Correlation",
        },
        "kwargs": {
            "colorscale": "Viridis",
            "colorbar": {
                "title": "Weather Sensitivity",
                "orientation": "h",
                "yanchor": "bottom",
                "y": -0.7,
                "xanchor": "center",
                "x": 0.5,
                "title_side": "bottom",
            },
            "zmin": -1,
            "zmax": 1,
        },
        "layout_kwargs": {
            "title": {
                "text": title,
                "x": 0.5,
                "y": 0.9,
                "xanchor": "center",
            },
            "xaxis_title": "Date",
            "yaxis_title": "Electricity Sensor",
            "font_color": "black",
            "plot_bgcolor": "white",
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
            "width": "49%",
            "display": "inline-block",
        },
    }

    output = WeatherSensitivity._prepare_data_for_vis(df_input, meter, title)

    pd.testing.assert_frame_equal(
        output["data_frame"].reset_index(drop=True),
        expected_output["data_frame"].reset_index(drop=True),
    )


def test_combine_meter_outside_temp_data():
    """test combine_meter_outside_temp_data function"""
    df_meters_data = pd.DataFrame(
        {
            "date": ["2024-01-01", "2024-01-02", "2024-01-03"],
            "consumption": [200, 250, 300],
        }
    )
    df_outside_temp = pd.DataFrame(
        {
            "date": ["2024-01-01", "2024-01-02", "2024-01-04"],
            "temperature": [15, 18, 21],
        }
    )

    expected_result = pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-01-01", "2024-01-02"]),
            "consumption": [200, 250],
            "temperature": [15, 18],
        }
    )

    result = WeatherSensitivity._combine_meter_outside_temp_data(
        df_meters_data, df_outside_temp
    )

    pd.testing.assert_frame_equal(
        result.reset_index(drop=True), expected_result.reset_index(drop=True)
    )


def test_get_daily_median_sensor_data():
    """test get_daily_median_sensor_data function"""
    df_sensors_data = pd.DataFrame(
        {
            "sensor_data": [
                [
                    {"timestamps": "2024-01-01 01:00", "values": 10},
                    {"timestamps": "2024-01-01 14:00", "values": 15},
                    {"timestamps": "2024-01-02 01:00", "values": 20},
                ],
                [
                    {"timestamps": "2024-01-01 02:00", "values": 5},
                    {"timestamps": "2024-01-01 15:00", "values": 7},
                    {"timestamps": "2024-01-02 02:00", "values": 10},
                ],
            ]
        }
    )

    expected_result = pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-01-01", "2024-01-02"]),
            "sensor1": [
                12.5,
                20.0,
            ],
            "sensor2": [
                6.0,
                10.0,
            ],
        }
    )

    result = WeatherSensitivity._get_daily_median_sensor_data(df_sensors_data)

    result["date"] = result["date"].astype("datetime64[ns]")
    expected_result["date"] = expected_result["date"].astype("datetime64[ns]")

    pd.testing.assert_frame_equal(
        result.reset_index(drop=True), expected_result.reset_index(drop=True)
    )


def test_load_sensors_from_db(mocker):
    """
    Unit test for the load_sensors_from_db method to ensure it loads sensor data
    from the database based on stream IDs in the provided DataFrame.
    """
    mock_db = mocker.Mock(spec=DBManager)

    sample_df = pd.DataFrame({"stream_id": ["stream1", "stream2"]})

    ws = WeatherSensitivity(mock_db)

    # Call the load_sensors_from_db method
    result_df = ws._load_sensors_from_db(sample_df)
    assert isinstance(result_df, pd.DataFrame), "Expected a DataFrame result"
    assert "sensor_data" in result_df.columns, "sensor_data column expected"


def test_get_data_from_rdf(mocker):
    """
    Unit test for the get_data_from_rdf method to verify that it retrieves and
    stores data in the rdf_data dictionary.
    """
    mock_db = mocker.Mock(spec=DBManager)

    ws = WeatherSensitivity(db=mock_db)

    ws._get_data_from_rdf()

    assert isinstance(ws.rdf_data, dict)
    keys = ws.rdf_data.keys()
    assert "electric_energy" in keys, "electric_energy data expected"
    assert "electric_power" in keys, "electric_power data expected"
    assert "gas" in keys, "gas data expected"
    assert "water" in keys, "water data expected"
    assert "chiller" in keys, "chiller data expected"
    assert "boiler" in keys, "boiler data expected"
    assert "outside_temp" in keys, "outside_temp data expected"


def test_get_sensor_data(mocker):
    """
    Unit test for the get_sensor_data method to verify that it loads sensor data
    for each type of data in rdf_data.
    """
    mock_db = mocker.Mock(spec=DBManager)

    ws = WeatherSensitivity(db=mock_db)
    sensor_data = ws._get_sensor_data()
    assert isinstance(sensor_data, dict)

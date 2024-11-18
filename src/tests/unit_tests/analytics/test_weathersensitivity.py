"""All the functions for Weather Sensitivity"""

import sys
from unittest.mock import MagicMock
import pandas as pd
from analytics.dbmgr import DBManager

# Silence irrelevant warnings
# pylint: disable=protected-access

import analytics.modules.weathersensitivity

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


def test_load_sensors_with_nan_stream_id(mocker):
    """
    Unit test for the load_sensors_from_db when the stream_id is missing.
    """
    mock_db = mocker.Mock(spec=DBManager)

    sample_df = pd.DataFrame({"stream_id": [None, None]})

    ws = WeatherSensitivity(mock_db)

    # Call the load_sensors_from_db method
    result_df = ws._load_sensors_from_db(sample_df)
    assert isinstance(result_df, pd.DataFrame), "Expected a DataFrame result"
    assert "sensor_data" in result_df.columns, "sensor_data column expected"


def test_load_sensors_from_nonempty_db(mocker):
    """
    Unit test for the load_sensors_from_db method to ensure it loads sensor data
    from the database based on stream IDs in the provided DataFrame.
    """
    weather_sensitivity = MagicMock()
    mock_db = mocker.Mock(spec=DBManager)

    sample_stream = pd.DataFrame(
        {
            "brick_class": ["sensor"],
            "time": ["2024-01-01 01:00"],
            "value": [10],
        }
    )
    mock_db.get_stream.return_value = sample_stream

    input_df = pd.DataFrame({"stream_id": ["stream_1", "stream_2"]})

    # Replace the mocked method with the actual implementation
    weather_sensitivity._load_sensors_from_db = WeatherSensitivity(
        mock_db
    )._load_sensors_from_db

    # Run the method under test
    result = weather_sensitivity._load_sensors_from_db(input_df)

    # Assert that the sensor_data column is not empty
    assert "sensor_data" in result.columns
    assert result.loc[1, "sensor_data"] is not None
    assert len(result) == 2


def test_load_sensors_with_keyerror(mocker):
    """
    Unit test for the load_sensors_from_db method when it encounters a KeyError.
    """
    weather_sensitivity = MagicMock()
    mock_db = mocker.Mock(spec=DBManager)
    mock_db.get_stream.side_effect = KeyError

    input_df = pd.DataFrame({"stream_id": ["stream_1", "stream_2"]})

    # Replace the mocked method with the actual implementation
    weather_sensitivity._load_sensors_from_db = WeatherSensitivity(
        mock_db
    )._load_sensors_from_db

    # Run the method under test
    result = weather_sensitivity._load_sensors_from_db(input_df)

    # Assert that the sensor_data column is empty
    assert "sensor_data" in result.columns
    assert result.loc[1, "sensor_data"] is None
    assert len(result) == 2


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


def test_get_sensor_data_empty_rdf(mocker):
    """
    Unit test for the get_sensor_data method to verify that it loads sensor data
    for each type of data in rdf_data.
    """
    mock_db = mocker.Mock(spec=DBManager)

    ws = WeatherSensitivity(db=mock_db)
    sensor_data = ws._get_sensor_data()
    assert isinstance(sensor_data, dict)


def test_get_sensor_data(mocker):
    """
    Unit test for the get_sensor_data method to verify that it loads sensor data
    for each type of data in rdf_data.
    """
    mock_db = mocker.Mock(spec=DBManager)

    ws = WeatherSensitivity(db=mock_db)
    ws.rdf_data = {
        "electric_energy": pd.DataFrame(
            {
                "meter": ["meter1"],
                "sensor": ["sensor1"],
                "stream_id": ["stream1"],
            }
        ),
        "electric_power": pd.DataFrame(
            {
                "meter": ["meter2"],
                "sensor": ["sensor2"],
                "stream_id": ["stream2"],
            }
        ),
        "gas": pd.DataFrame(
            {
                "meter": ["meter3"],
                "sensor": ["sensor3"],
                "stream_id": ["stream3"],
            }
        ),
        "water": pd.DataFrame(
            {
                "meter": ["meter4"],
                "sensor": ["sensor4"],
                "stream_id": ["stream4"],
            }
        ),
        "chiller": pd.DataFrame(
            {
                "meter": ["meter5"],
                "sensor": ["sensor5"],
                "stream_id": ["stream5"],
            }
        ),
        "boiler": pd.DataFrame(
            {
                "meter": ["meter6"],
                "sensor": ["sensor6"],
                "stream_id": ["stream6"],
            }
        ),
        "outside_temp": pd.DataFrame(
            {
                "sensor": ["sensor7"],
                "stream_id": ["stream7"],
            }
        ),
    }

    sensor_data = ws._get_sensor_data()
    assert isinstance(sensor_data, dict)
    assert "electric_energy" in sensor_data
    assert "electric_power" in sensor_data
    assert "gas" in sensor_data
    assert "water" in sensor_data
    assert "chiller" in sensor_data
    assert "boiler" in sensor_data
    assert "outside_temp" in sensor_data


def test_get_daily_median_outside_temperature():
    """
    Unit test for the _get_daily_median_outside_temperature method to verify that
    it calculates the daily median outside temperature from the nested DataFrame.
    """
    nested_df = pd.DataFrame(
        {
            "timestamps": ["2024-01-01", "2024-01-02", "2024-01-03"],
            "values": [10.0, 20.0, 30.0],
        }
    )

    input_df = pd.DataFrame(
        {
            "sensor_data": [
                nested_df,
            ]
        }
    )

    df = WeatherSensitivity._get_daily_median_outside_temperature(input_df)

    assert "date" in df.columns
    assert "outside_temp" in df.columns
    assert len(df) == 3
    assert df.loc[0, "outside_temp"] == 10.0


def test_get_weather_sensitivity_single_digit_sensors():
    """
    Unit test for the _get_weather_sensitivity method to verify that it calculates
    the correlation between outside temperature and sensor data when there are
    fewer than 10 sensors.
    """
    input_df = pd.DataFrame(
        {
            "date": pd.date_range(start="2024-01-01", periods=5, freq="W"),
            "outside_temp": [10.0, 15.0, 20.0, 25.0, 30.0],
            "sensor1": [0.8, 0.6, 0.5, 0.7, 0.9],
        }
    )

    df = WeatherSensitivity._get_weather_sensitivity(input_df)

    assert "date" in df.columns


def test_get_weather_sensitivity_double_digit_sensors():
    """
    Unit test for the _get_weather_sensitivity method to verify that it calculates
    the correlation between outside temperature and sensor data when there are
    at least 10 sensors.
    """
    input_df = pd.DataFrame(
        {
            "date": pd.date_range(start="2024-01-01", periods=5, freq="W"),
            "outside_temp": [10.0, 15.0, 20.0, 25.0, 30.0],
            "sensor1": [0.8, 0.6, 0.5, 0.7, 0.9],
            "sensor2": [0.8, 0.6, 0.5, 0.7, 0.9],
            "sensor3": [0.8, 0.6, 0.5, 0.7, 0.9],
            "sensor4": [0.8, 0.6, 0.5, 0.7, 0.9],
            "sensor5": [0.8, 0.6, 0.5, 0.7, 0.9],
            "sensor6": [0.8, 0.6, 0.5, 0.7, 0.9],
            "sensor7": [0.8, 0.6, 0.5, 0.7, 0.9],
            "sensor8": [0.8, 0.6, 0.5, 0.7, 0.9],
            "sensor9": [0.8, 0.6, 0.5, 0.7, 0.9],
            "sensor10": [0.8, 0.6, 0.5, 0.7, 0.9],
        }
    )

    df = WeatherSensitivity._get_weather_sensitivity(input_df)

    assert "date" in df.columns


def test_get_daily_median_data():
    """
    Unit test for the _get_daily_median_data method to verify that it calculates
    the daily median of sensor data from the nested DataFrames.
    """
    nested_df = pd.DataFrame(
        {
            "timestamps": ["2024-01-01", "2024-01-02", "2024-01-03"],
            "values": [10.0, 20.0, 30.0],
        }
    )

    input_df = pd.DataFrame(
        {
            "sensor_data": [
                nested_df,
            ]
        }
    )

    input_dict = {
        "outside_temp": input_df,
        "sensor1": input_df,
    }

    results = WeatherSensitivity._get_daily_median_data(input_dict)

    assert isinstance(results, dict)
    assert "outside_temp" in results
    assert "sensor1" in results


def test_combine_meter_weather_data():
    """
    Unit test for the _combine_meter_weather_data method to verify that it combines
    the meter data with the outside temperature data.
    """
    meter_data = pd.DataFrame(
        {
            "date": pd.date_range(start="2024-01-01", periods=5, freq="W"),
            "consumption": [200, 250, 300, 350, 400],
        }
    )

    outside_temp_data = pd.DataFrame(
        {
            "date": pd.date_range(start="2024-01-01", periods=5, freq="W"),
            "outside_temp": [10.0, 15.0, 20.0, 25.0, 30.0],
        }
    )

    input_dict = {
        "meter": meter_data,
        "outside_temp": outside_temp_data,
    }

    results = WeatherSensitivity._combine_meter_weather_data(input_dict)

    assert "meter" in results
    assert len(results) == 1


def test_get_weather_sensitivity_results():
    """
    Unit test for the _get_weather_sensitivity_results method to verify that it
    calculates the correlation between meter data and outside temperature data.
    """
    meter_df = pd.DataFrame(
        {
            "date": pd.date_range(start="2024-01-01", periods=5, freq="W"),
            "outside_temp": [10.0, 15.0, 20.0, 25.0, 30.0],
            "sensor1": [0.8, 0.6, 0.5, 0.7, 0.9],
        }
    )

    input_dict = {
        "meter1": meter_df,
        "meter2": meter_df,
    }

    results = WeatherSensitivity._get_weather_sensitivity_results(input_dict)

    assert "meter1" in results
    assert "meter2" in results
    assert len(results) == 2


def test_get_data_for_dash():
    """
    Unit test for the get_data_for_dash method to verify that it prepares the
    data for visualization in the dashboard.
    """
    meter_df = pd.DataFrame(
        {
            "date": pd.date_range(start="2024-01-01", periods=5, freq="W"),
            "year_month": ["2024-01", "2024-02", "2024-03", "2024-04", "2024-05"],
            "sensor1": [0.8, 0.6, 0.5, 0.7, 0.9],
        }
    )

    input_dict = {
        "meter1": meter_df,
        "meter2": meter_df,
    }

    results = WeatherSensitivity._get_data_for_dash(input_dict)

    assert ("WeatherSensitivity", "Correlation Analysis") in results
    assert "components" in results[("WeatherSensitivity", "Correlation Analysis")]
    assert (
        len(results[("WeatherSensitivity", "Correlation Analysis")]["components"]) == 2
    )


def test_run(mocker):
    """
    Unit test for the run method to verify that it returns the expected results.
    """
    mock_db = mocker.Mock(spec=DBManager)

    mocker.patch(
        "analytics.modules.weathersensitivity.WeatherSensitivity._get_data_from_rdf",
        return_value=None,
    )

    mocker.patch(
        "analytics.modules.weathersensitivity.WeatherSensitivity._get_daily_median_data",
        return_value={},
    )

    mocker.patch(
        "analytics.modules.weathersensitivity.WeatherSensitivity._combine_meter_weather_data",
        return_value={},
    )

    mocker.patch(
        "analytics.modules.weathersensitivity.WeatherSensitivity._get_weather_sensitivity_results",
        return_value={},
    )

    results = analytics.modules.weathersensitivity.run(mock_db)

    assert ("WeatherSensitivity", "Correlation Analysis") in results
    assert "components" in results[("WeatherSensitivity", "Correlation Analysis")]
    assert (
        len(results[("WeatherSensitivity", "Correlation Analysis")]["components"]) == 0
    )

"""
Unit tests for the consumption module in the analytics package.
This module tests the `get_building_meters` and `run` functions to ensure they
correctly retrieve and process building meter data.
"""

import pandas as pd
from analytics.dbmgr import DBManager
import analytics.modules.consumption as cons

def test_building_meters_and_default_units(mocker):
    """
    Unit test for the `_get_building_meters` function in the consumption module.
    This test verifies that the function correctly retrieves building meter data,
    assigns default units for gas and water meters when missing, and handles
    cases where the unit is None for other meter types.
    """
    # Mock DBManager instance and query results
    mock_db = mocker.Mock(spec=DBManager)

    # Sample data simulating a database response
    meter_data = pd.DataFrame(
        {
            "equipment": ["meter1", "meter2", "meter3", "meter4"],
            "equipment_type": [
                "Electrical_Meter",
                "Building_Water_Meter",
                "Building_Gas_Meter",
                "Unknown_Meter_Type",
            ],
            "sensor": ["sensor1", "sensor2", "sensor3", "sensor4"],
            "sensor_type": [
                "Electrical_Power_Sensor",
                "Usage_Sensor",
                "Usage_Sensor",
                "Electrical_Power_Sensor",
            ],
            "unit": [
                "kWh",  # Valid unit
                None,  # Missing unit for water meter
                None,  # Missing unit for gas meter
                None,  # Missing unit for an unknown meter type
            ],
            "stream_id": ["stream1", "stream2", "stream3", "stream4"],
        }
    )

    # Configure the mock DBManager to return the sample data
    mock_db.query.return_value = meter_data

    # Mock `get_stream` to return sample time-series data for each stream ID
    def mock_get_stream(stream_id):
        return pd.DataFrame(
            {
                "time": pd.date_range(start="2024-01-01", periods=10, freq="10min"),
                "brick_class": ["value"] * 10,
                "value": range(10),
            }
        )

    mock_db.get_stream.side_effect = mock_get_stream

    # Call the function to retrieve building meters
    result_df = cons._get_building_meters(mock_db)

    # Assertions for data retrieval
    assert isinstance(result_df, pd.DataFrame), "Expected a DataFrame result"
    pd.testing.assert_frame_equal(result_df, meter_data)
    mock_db.query.assert_called_once()

    # Run the main consumption configuration to check default unit assignment
    config = cons.run(mock_db)

    # Verify that the default unit is correctly assigned as "Cubic meters" for missing units
    assert (
        config[("Consumption", "BuildingWaterMeter")]["components"][0]["kwargs"][
            "data_frame"
        ]["Sensor"].iloc[0]
        == "Building Water Meter Combined (Cubic meters)"
    )
    assert (
        config[("Consumption", "BuildingGasMeter")]["components"][0]["kwargs"][
            "data_frame"
        ]["Sensor"].iloc[0]
        == "Building Gas Meter Combined (Cubic meters)"
    )

    # Verify that the "unit = None" case for unknown meters does not raise errors
    assert ("Consumption", "UnknownMeterType") in config
    assert (
        config[("Consumption", "UnknownMeterType")]["components"][0]["kwargs"][
            "data_frame"
        ]["Sensor"].iloc[0]
        == "Unknown Meter Type Combined (None)"
    )


def test_get_building_meters_empty_result(mocker):
    """
    Unit test for the `_get_building_meters` function to handle an empty
    database response. Verifies that an empty DataFrame is returned
    when no data is available.
    """

    # Mock DBManager instance and query results
    mock_db = mocker.Mock(spec=DBManager)

    # Empty DataFrame as query result
    mock_db.query.return_value = pd.DataFrame()

    # Call the function
    result_df = cons._get_building_meters(mock_db)

    # Check if the returned DataFrame is empty
    assert result_df.empty


def test_run_empty_db(mocker):
    """
    Unit test for the `run` function in the consumption module with an empty
    database response. Ensures that the function returns an empty configuration
    dictionary when no meter data is available.
    """

    # Mock DBManager instance and query results
    mock_db = mocker.Mock(spec=DBManager)

    # Mock _get_building_meters to return an empty DataFrame
    mocker.patch(
        "analytics.modules.consumption._get_building_meters",
        return_value=pd.DataFrame(),
    )

    # Run the function and capture the result
    result = cons.run(mock_db)

    # Assertion: Check that the function returns an empty dictionary
    assert not result


def test_run_with_meters(mocker):
    """
    Integration test for the `run` function in the consumption module with
    sample meter data. This test verifies that the function correctly processes
    data, handles combined_data logic, and returns a configuration dictionary
    for visualization.
    """

    # Mock DBManager instance
    mock_db = mocker.Mock(spec=DBManager)

    # Mock data_meters with multiple sensors for the same meter type
    meter_data = pd.DataFrame(
        {
            "equipment": ["meter1", "meter1", "meter2"],
            "equipment_type": ["Electrical_Meter", "Electrical_Meter", "Gas_Meter"],
            "sensor": ["sensor1", "sensor2", "sensor3"],
            "sensor_type": ["Electrical_Power_Sensor", "Electrical_Power_Sensor", "Usage_Sensor"],
            "unit": ["kWh", "kWh", "cubic meters"],
            "stream_id": ["stream1", "stream2", "stream3"],
        }
    )

    # Configure the mock DBManager to return sample meter data
    mock_db.query.return_value = meter_data

    # Mock get_stream to return overlapping time series data
    def mock_get_stream(stream_id):
        if stream_id == "stream1":
            return pd.DataFrame(
                {
                    "time": pd.date_range(start="2024-01-01", periods=10, freq="10min"),
                    "brick_class": ["value"] * 10,
                    "value": range(10),
                }
            ).reset_index(drop=True)
        elif stream_id == "stream2":
            return pd.DataFrame(
                {
                    "time": pd.date_range(start="2024-01-01", periods=10, freq="10min"),
                    "brick_class": ["value"] * 10,
                    "value": range(5, 15),
                }
            ).reset_index(drop=True)
        else:  # stream3
            return pd.DataFrame(
                {
                    "time": pd.date_range(start="2024-01-01", periods=10, freq="10min"),
                    "brick_class": ["value"] * 10,
                    "value": range(10, 20),
                }
            ).reset_index(drop=True)

    mock_db.get_stream.side_effect = mock_get_stream

    # Run the function
    config = cons.run(mock_db)

    # Assertions
    # Verify that the configuration dictionary contains keys for both meter types
    assert ("Consumption", "ElectricalMeter") in config
    assert ("Consumption", "GasMeter") in config

    # Verify the combined_data logic
    electrical_data = config[("Consumption", "ElectricalMeter")]["components"][0]["kwargs"]["data_frame"]
    assert electrical_data["Usage"].iloc[0] == 5 
    assert electrical_data["Usage"].iloc[1] == 7

    gas_data = config[("Consumption", "GasMeter")]["components"][0]["kwargs"]["data_frame"]
    assert gas_data["Usage"].iloc[0] == 10

    # Check that each key has the necessary component structure
    for key in config:
        assert "components" in config[key]
        assert len(config[key]["components"]) > 0


def test_run_with_meters_cover_branches(mocker):
    """
    Integration test for the `run` function in the consumption module with
    sample meter data. This test verifies that the function correctly processes
    data, handles combined_data logic, appends components for existing config keys,
    and skips groups with no combined data.
    """

    # Mock DBManager instance
    mock_db = mocker.Mock(spec=DBManager)

    # Mock data_meters with multiple sensors for the same meter type and a sensor with no data
    meter_data = pd.DataFrame(
        {
            "equipment": ["meter1", "meter1", "meter2"],
            "equipment_type": ["Electrical_Meter", "Electrical_Meter", "Gas_Meter"],
            "sensor": ["sensor1", "sensor2", "sensor3"],
            "sensor_type": ["Electrical_Power_Sensor", "Electrical_Energy_Sensor", "Usage_Sensor"],
            "unit": ["kWh", "kWh", "cubic meters"],
            "stream_id": ["stream1", "stream2", "stream3"],
        }
    )

    # Configure the mock DBManager to return sample meter data
    mock_db.query.return_value = meter_data

    # Mock get_stream to return time series data and one empty DataFrame
    def mock_get_stream(stream_id):
        if stream_id == "stream1":
            return pd.DataFrame(
                {
                    "time": pd.date_range(start="2024-01-01", periods=10, freq="10min"),
                    "brick_class": ["value"] * 10,
                    "value": range(10),
                }
            )
        elif stream_id == "stream2":
            return pd.DataFrame(
                {
                    "time": pd.date_range(start="2024-01-01", periods=10, freq="10min"),
                    "brick_class": ["value"] * 10,
                    "value": range(5, 15),
                }
            )
        else:  # stream3 returns no data
            return pd.DataFrame()

    mock_db.get_stream.side_effect = mock_get_stream

    # Run the function
    config = cons.run(mock_db)

    # Assertions
    # Verify that the configuration dictionary contains keys for ElectricalMeter
    assert ("Consumption", "ElectricalMeter") in config
    assert ("Consumption", "GasMeter") not in config  # GasMeter has no combined data

    # Verify that multiple components are appended for ElectricalMeter
    electrical_meter_components = config[("Consumption", "ElectricalMeter")]["components"]
    assert len(electrical_meter_components) > 1, "Expected multiple components for ElectricalMeter"

    # Verify combined values for the first sensor in ElectricalMeter
    electrical_data = electrical_meter_components[0]["kwargs"]["data_frame"]
    assert electrical_data["Usage"].iloc[0] == 5

    # Verify that the GasMeter group was skipped due to no data
    assert "Consumption" not in config or ("Consumption", "GasMeter") not in config

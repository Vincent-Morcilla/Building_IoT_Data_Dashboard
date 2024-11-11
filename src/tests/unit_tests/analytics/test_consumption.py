"""
Unit tests for the consumption module in the analytics package.
This module tests the `get_building_meters` and `run` functions to ensure they
correctly retrieve and process building meter data.
"""

import pandas as pd
from analytics.dbmgr import DBManager
import analytics.modules.consumption as cons


def test_get_building_meters_success(mocker):
    """
    Unit test for the `_get_building_meters` function in the consumption module.
    This test verifies that the function correctly retrieves building meter data
    from a mock DBManager instance with sample data.
    """
    # Mock DBManager instance and query results
    mock_db = mocker.Mock(spec=DBManager)

    # Sample data simulating a database response
    expected_df = pd.DataFrame(
        {
            "equipment": ["meter1", "meter2"],
            "equipment_type": ["ElectricalMeter", "GasMeter"],
            "sensor": ["sensor1", "sensor2"],
            "sensor_type": ["ElectricalPowerSensor", "UsageSensor"],
            "unit": ["kWh", "cubic meters"],
            "stream_id": ["stream1", "stream2"],
        }
    )

    # Configure the mock DBManager to return the sample data
    mock_db.query.return_value = expected_df

    # Call the function
    result_df = cons._get_building_meters(mock_db)

    # Assertions
    # Check that the result is a DataFrame
    assert isinstance(result_df, pd.DataFrame), "Expected a DataFrame result"

    # Check that the result matches the sample data structure and values
    pd.testing.assert_frame_equal(result_df, expected_df)

    # Verify the database query was called as expected
    mock_db.query.assert_called_once()


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
    data and returns a configuration dictionary for visualization.
    """

    # Mock DBManager instance
    mock_db = mocker.Mock(spec=DBManager)

    # Mock data_meters with two types of meters
    meter_data = pd.DataFrame(
        {
            "equipment": ["meter1", "meter2"],
            "equipment_type": ["ElectricalMeter", "GasMeter"],
            "sensor": ["sensor1", "sensor2"],
            "sensor_type": ["ElectricalPowerSensor", "UsageSensor"],
            "unit": ["kWh", "cubic meters"],
            "stream_id": ["stream1", "stream2"],
        }
    )

    # Configure the mock DBManager to return sample meter data
    mock_db.query.return_value = meter_data

    # Mock get_stream to return time series data
    mock_db.get_stream.return_value = pd.DataFrame(
        {
            "time": pd.date_range(start="2024-01-01", periods=10, freq="10min"),
            "brick_class": ["value"] * 10,
            "value": range(10),
        }
    )

    # Run the function
    config = cons.run(mock_db)

    # Assertions
    # Verify that the configuration dictionary contains keys for both meter types
    assert ("Consumption", "ElectricalMeter") in config
    assert ("Consumption", "GasMeter") in config

    # Check that each key has the necessary component structure
    for key in config:
        assert "components" in config[key]
        assert len(config[key]["components"]) > 0

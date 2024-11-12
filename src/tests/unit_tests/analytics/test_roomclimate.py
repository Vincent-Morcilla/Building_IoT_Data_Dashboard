"""Unit tests for the roomclimate module in the analytics package."""

import pandas as pd

from analytics.dbmgr import DBManager
import analytics.modules.roomclimate as rc


def test_get_rooms_with_temp(mocker):
    """
    Unit test for the get_rooms_with_temp function in the roomclimate module
    using a mock DBManager instance.
    """
    # Mock DBManager instance and query results
    mock_db = mocker.Mock(spec=DBManager)

    # Sample data simulating a database response
    sample_data = pd.DataFrame(
        {
            "room_id": ["room1", "room2"],
            "room_class": ["class1", "class2"],
            "ats": ["sensor1", "sensor2"],
            "ats_stream": ["stream1", "stream2"],
            "atsp": ["setpoint1", "setpoint2"],
            "atsp_stream": ["stream3", "stream4"],
        }
    )

    # Configure the mock DBManager to return the sample data
    mock_db.query.return_value = sample_data

    # The function under test is protected, so we disable the protected access
    # warning and call the function directly
    # pylint: disable=protected-access
    result = rc._get_rooms_with_temp(mock_db)

    # Assertions
    # Check that the result is a DataFrame
    assert isinstance(result, pd.DataFrame), "Expected a DataFrame result"

    # Check that the result matches the sample data structure and values
    pd.testing.assert_frame_equal(result, sample_data)

    # Verify the database query was called as expected
    mock_db.query.assert_called_once()


def test_get_outside_air_temp(mocker):
    """
    Unit test for the get_outside_air_temp function in the roomclimate module
    using a mock DBManager instance.
    """
    # Mock DBManager instance
    mock_db = mocker.Mock(spec=DBManager)

    # Sample data simulating a database response
    sample_data = pd.DataFrame(
        {"oats": ["sensor_outside"], "oats_stream": ["stream_outside"]}
    )

    # Configure the mock DBManager to return the sample data
    mock_db.query.return_value = sample_data

    # The function under test is protected, so we disable the protected access
    # warning and call the function directly
    # pylint: disable=protected-access
    result = rc._get_outside_air_temp(mock_db)

    # Assertions
    assert isinstance(result, pd.DataFrame), "Expected a DataFrame result"
    pd.testing.assert_frame_equal(result, sample_data)
    mock_db.query.assert_called_once()


def test_build_components():
    """
    Unit test for the build_components function in the roomclimate module
    using a sample DataFrame.
    """
    # Sample DataFrame representing timeseries data
    sample_df = pd.DataFrame(
        {"Date": ["2023-01-01", "2023-01-02"], "Temperature": [20.5, 21.0]}
    )
    room_id = "room1"
    title = "Room 1 Temperature Over Time"

    # The function under test is protected, so we disable the protected access
    # warning and call the function directly
    # pylint: disable=protected-access
    result = rc._build_components(sample_df, room_id, title)

    # Assertions
    assert isinstance(result, list), "Expected a list result"
    assert len(result) == 1, "Expected one plot component"

    # Check the structure and contents of the plot configuration
    component = result[0]
    assert component["type"] == "plot"
    assert component["library"] == "px"
    assert component["function"] == "line"
    assert component["id"] == f"roomclimate-line-plot-{room_id}"
    assert component["kwargs"]["data_frame"].equals(
        sample_df
    ), "DataFrame mismatch in plot configuration"
    assert component["kwargs"]["x"] == "Date"
    assert component["kwargs"]["y"].equals(sample_df.columns), "Y-axis column mismatch"
    assert component["layout_kwargs"]["title"]["text"] == title


def test_run_empty_db(mocker):
    """
    Unit test for the run function in the roomclimate module with an empty
    database response.
    """
    mock_db = mocker.Mock(spec=DBManager)
    # Mock _get_rooms_with_temp to return an empty DataFrame
    mocker.patch(
        "analytics.modules.roomclimate._get_rooms_with_temp",
        return_value=pd.DataFrame(),
    )

    result = rc.run(mock_db)

    assert ("RoomClimate", "Error") in result
    assert "components" in result[("RoomClimate", "Error")]
    assert len(result[("RoomClimate", "Error")]["components"]) == 1
    assert "type" in result[("RoomClimate", "Error")]["components"][0]
    assert result[("RoomClimate", "Error")]["components"][0]["type"] == "error"


def test_run_with_rooms_no_outside_air(mocker):
    """
    Unit test for the run function in the roomclimate module with a database
    response containing room data but no outside air temperature data.
    """
    # Mock DBManager instance
    mock_db = mocker.Mock(spec=DBManager)

    # Sample room data
    sample_rooms = pd.DataFrame(
        {
            "room_id": ["room1"],
            "room_class": ["office"],
            "ats": ["sensor1"],
            "ats_stream": ["stream1"],
            "atsp": ["setpoint1"],
            "atsp_stream": ["stream2"],
        }
    )

    # Mock _get_rooms_with_temp to return sample room data
    mocker.patch(
        "analytics.modules.roomclimate._get_rooms_with_temp",
        return_value=sample_rooms,
    )

    # Mock _get_outside_air_temp to return an empty DataFrame
    mocker.patch(
        "analytics.modules.roomclimate._get_outside_air_temp",
        return_value=pd.DataFrame(),
    )

    # Configure the mock DBManager to return some sample timeseries data
    mock_db.get_stream.side_effect = lambda stream_id: pd.DataFrame(
        {
            "time": pd.date_range("2023-01-01", periods=3, freq="h"),
            "brick_class": [f"{stream_id}_class"] * 3,
            "value": [20.5, 21.0, 22.0],
        }
    )

    # Run the function under test
    result = rc.run(mock_db)

    # Assertions
    assert isinstance(result, dict)
    assert (
        "room1"
        in result[("RoomClimate", "RoomClimate")]["interactions"][0]["data_source"][
            "data_dict"
        ]
    )

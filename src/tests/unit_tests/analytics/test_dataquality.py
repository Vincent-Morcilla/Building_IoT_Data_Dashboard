"""Unit tests for the dataquality module in the analytics package."""

import numpy as np
import pandas as pd
import datetime
import pytest

from analytics.dbmgr import DBManager
import analytics.modules.dataquality as dq

@pytest.fixture
def mock_db(mocker):
    """Create a mock database manager."""
    mock = mocker.Mock(spec=DBManager)
    mock.__getitem__ = mocker.Mock()
    return mock


def test_detect_outliers_edge_cases():
    """Test outlier detection with edge cases."""
    # Test with constant values
    values = np.array([1.0, 1.0, 1.0, 1.0])
    outliers, _ = dq._detect_outliers(values)
    assert outliers == 0

    # Test with single value
    values = np.array([1.0])
    outliers, _ = dq._detect_outliers(values)
    assert outliers == 0

    # Test with NaN values
    values = np.array([1.0, np.nan, 2.0, 3.0])
    outliers, _ = dq._detect_outliers(values)
    assert not np.isnan(outliers)


def test_deduce_granularity():
    """Test granularity deduction from timestamps."""
    # Test hourly data
    timestamps = pd.date_range(start="2024-01-01", periods=5, freq="h")
    assert dq._deduce_granularity(timestamps) == "1 hours"

    # Test daily data
    timestamps = pd.date_range(start="2024-01-01", periods=5, freq="D")
    assert dq._deduce_granularity(timestamps) == "1 days"

    # Test empty data
    timestamps = pd.Series([], dtype="datetime64[ns]")
    assert dq._deduce_granularity(timestamps) is None


def test_analyse_sensor_gaps():
    """Test gap analysis with different gap patterns."""
    # Create timestamps with a gap in the middle
    timestamps = pd.date_range(start="2024-01-01", periods=5, freq="h")
    timestamps = pd.Series(
        timestamps[:2].append(timestamps[3:])
    )  # Remove middle timestamp

    df = pd.DataFrame(
        {
            "Timestamps": pd.Series([timestamps]),
            "Deduced_Granularity": ["1 hours"],
            "Values": [[1.0] * len(timestamps)],
        },
        index=[0],
    )

    result = dq._analyse_sensor_gaps(df)

    # Add assertions to verify gap detection
    assert isinstance(result, pd.DataFrame)
    assert "Small_Gap_Count" in result.columns
    assert result["Small_Gap_Count"].iloc[0] == 1  # Should detect one small gap
    assert result["Total_Gaps"].iloc[0] == 1


def test_profile_groups():
    """Test group profiling and outlier detection."""
    n_normal = 20
    df = pd.DataFrame(
        {
            "Label": ["Temp"] * (n_normal + 1),
            "Sensor_Mean": [1.0] * n_normal
            + [30.0],  # 20 normal sensors at 1.0, 1 outlier at 30.0
            "stream_id": [f"s{i}" for i in range(1, n_normal + 2)],
            "Values": [[1.0]] * n_normal + [[500.0]],
            "Timestamps": [pd.date_range("2024-01-01", periods=1)] * (n_normal + 1),
            "Value_Count": [100] * (n_normal + 1),
            "Missing": [0] * (n_normal + 1),
            "Zeros": [0] * (n_normal + 1),
        }
    )

    result = dq._profile_groups(df)
    assert "Flagged For Removal" in result.columns
    print(result)
    assert (
        result.loc[
            result["stream_id"] == f"s{n_normal + 1}", "Flagged For Removal"
        ].iloc[0]
        > 0
    )


def test_preprocess_to_sensor_rows(mocker):
    """Test preprocessing of sensor data."""
    mock_db = mocker.Mock(spec=DBManager)

    # Mock stream data
    stream_data = pd.DataFrame(
        {
            "time": pd.date_range(start="2024-01-01", periods=5, freq="h"),
            "value": [1.0, 1.0, 2.0, 2.0, 2.0],
        }
    )

    mock_db.get_all_streams.return_value = {"stream1": stream_data}
    mock_db.get_label.return_value = "Temperature_Sensor"

    result = dq._preprocess_to_sensor_rows(mock_db)

    assert isinstance(result, pd.DataFrame)
    assert "stream_id" in result.columns
    assert "Label" in result.columns
    assert "Is_Step_Function" in result.columns


def test_prepare_data_quality_df():
    """Test preparation of data quality DataFrame."""
    input_df = pd.DataFrame(
        {
            "stream_id": ["s1", "s2"],
            "Label": ["Temp", "Humidity"],
            "Value_Count": [100.0, 200.0],
            "Outliers": [5.0, 0.0],
            "Missing": [2.0, 1.0],
            "Zeros": [0.0, 3.0],
            "Start_Timestamp": [pd.Timestamp("2024-01-01"), pd.Timestamp("2024-01-01")],
            "End_Timestamp": [pd.Timestamp("2024-01-02"), pd.Timestamp("2024-01-02")],
            "Sensor_Mean": [22.5, 45.0],
            "Sensor_Min": [20.0, 40.0],
            "Sensor_Max": [25.0, 50.0],
            "Flagged For Removal": [0.0, 0.0],
            "Is_Step_Function": [False, True],
            "Deduced_Granularity": ["1 hours", "1 hours"],
            "Gap_Percentage": pd.Series([0.1, 0.2]),
            "Total_Gap_Size_Seconds": [3600.0, 7200.0],
            "Group_Mean": pd.Series([22.5, 45.0]),
            "Group_Std": pd.Series([1.5, 2.0]),
            "Percentage_Flat_Regions": pd.Series([95.0, 85.0]),
            "Unique_Values_Count": [2, 3],
            "Small_Gap_Count": [1, 1],
            "Medium_Gap_Count": [0, 1],
            "Large_Gap_Count": [0, 0],
            "Total_Gaps": [1, 2],
        }
    )

    result = dq._prepare_data_quality_df(input_df)

    assert "Stream ID" in result.columns
    assert "Brick Class" in result.columns


def test_get_data_quality_overview():
    """Test generation of data quality overview."""
    data_quality_df = pd.DataFrame(
        {
            "Stream ID": ["stream1", "stream2"],
            "Brick Class": ["Temperature_Sensor", "Humidity_Sensor"],
            "Outliers": [0, 5],
            "Start Timestamp": [pd.Timestamp("2024-01-01"), pd.Timestamp("2024-01-01")],
            "End Timestamp": [pd.Timestamp("2024-01-02"), pd.Timestamp("2024-01-02")],
            "Is Step Function": [True, False],
            "Total Gaps": [1, 2],
            "Gap Percentage": [0.1, 0.2],
            "Sample Rate": ["1 hours", "1 hours"],
            "Samples": [24, 24],
            "Small Gaps": [1, 1],
            "Medium Gaps": [0, 1],
            "Large Gaps": [0, 0],
            "Group Mean": [22.5, 45.0],
            "Group Std": [1.5, 2.0],
            "Flagged For Removal": [0, 1],
        }
    )

    result = dq._get_data_quality_overview(data_quality_df)
    assert "tables" in result


def test_detect_step_function_behavior():
    """Test step function detection with various input cases."""
    # Test case 1: Empty/short array
    values = np.array([1.0])
    result = dq._detect_step_function_behavior(values)
    assert result == {
        "percentage_flat": None,
        "unique_values_count": None,
        "is_step_function": None,
    }

    # Test case 2: Clear step function with >90% flat regions
    values = np.array([1.0] * 95 + [2.0] * 5)  # 95% flat regions
    result = dq._detect_step_function_behavior(
        values, percentage_threshold=0.1, unique_values_threshold=5
    )
    assert result["is_step_function"] == True
    assert result["unique_values_count"] == 2


def test_create_summary_table():
    """Test creation of summary statistics table."""
    data_quality_df = pd.DataFrame(
        {
            "Brick Class": ["Temp", "Temp", "Humidity"],
            "Stream ID": ["s1", "s2", "s3"],
            "Sample Rate": ["1 hours"] * 3,
            "Start Timestamp": [pd.Timestamp("2024-01-01")] * 3,
            "End Timestamp": [pd.Timestamp("2024-01-02")] * 3,
            "Samples": [24, 24, 24],
            "Outliers": [2, 1, 0],
            "Missing": [1, 0, 1],
            "Zeros": [0, 0, 2],
            "Small Gaps": [1, 0, 1],
            "Medium Gaps": [0, 1, 0],
            "Large Gaps": [0, 0, 1],
            "Total Gaps": [1, 1, 2],
            "Group Mean": [22.5, 22.5, 45.0],
            "Group Std": [1.5, 1.5, 2.0],
            "Total Gap Size (s)": [3600, 7200, 14400],
            "Flagged For Removal": [0, 0, 1],
            "Is Step Function": [False, False, True],
        }
    )

    result = dq._create_summary_table(data_quality_df)

    assert "Number of Streams" in result.columns
    assert "Gap Percentage" in result.columns
    assert "Step Function Percentage" in result.columns
    assert len(result) == 2  # Should have 2 groups (Temp and Humidity)


def test_run_empty_db(mocker):
    """Test run function with empty database."""
    mock_db = mocker.Mock(spec=DBManager)

    # Mock _preprocess_to_sensor_rows to return an empty DataFrame
    mocker.patch(
        "analytics.modules.dataquality._preprocess_to_sensor_rows",
        return_value=pd.DataFrame(),
    )

    result = dq.run(mock_db)
    assert not result


def test_run_with_data(mock_db):
    """Test run function with sample data."""
    stream_data = pd.DataFrame(
        {
            "time": pd.date_range(start="2024-01-01", periods=5, freq="h"),
            "value": [1.0, 1.0, 2.0, 2.0, 2.0],
            "brick_class": ["Temperature_Sensor"] * 5,
        }
    )

    mock_db.get_all_streams.return_value = {"stream1": stream_data}
    mock_db.get_label.return_value = "Temperature_Sensor"
    mock_db.get_stream.return_value = stream_data

    result = dq.run(mock_db)
    assert isinstance(result, dict)


def test_plot_config_structure(mock_db):
    """Test the structure of the plot configuration."""
    stream_data = pd.DataFrame(
        {
            "time": pd.date_range(start="2024-01-01", periods=5, freq="h"),
            "value": [1.0, 1.0, 2.0, 2.0, 2.0],
            "brick_class": ["Temperature_Sensor"] * 5,
            "Label": ["Temperature_Sensor"] * 5,
        }
    )
    mock_db.get_all_streams.return_value = {"stream1": stream_data}
    mock_db.get_label.return_value = "Temperature_Sensor"
    mock_db.get_stream.return_value = stream_data
    mock_db.__getitem__.return_value = stream_data

    result = dq.run(mock_db)

    # Test Overview section
    overview = result[("DataQuality", "Overview")]
    assert "components" in overview
    assert any(c["type"] == "table" for c in overview["components"])

    # Test ByClass section
    by_class = result[("DataQuality", "ByClass")]
    assert "components" in by_class
    assert "interactions" in by_class

    # Test ByStream section
    by_stream = result[("DataQuality", "ByStream")]
    assert "components" in by_stream
    assert "interactions" in by_stream


def test_table_styling(mock_db):
    """Test that table styling is correctly configured."""
    stream_data = pd.DataFrame(
        {
            "time": pd.date_range(start="2024-01-01", periods=5, freq="h"),
            "value": [1.0, 1.0, 2.0, 2.0, 2.0],
            "brick_class": ["Temperature_Sensor"] * 5,
        }
    )
    mock_db.get_all_streams.return_value = {"stream1": stream_data}
    mock_db.get_label.return_value = "Temperature_Sensor"
    mock_db.get_stream.return_value = stream_data

    result = dq.run(mock_db)

    for section in result.values():
        for component in section["components"]:
            if component.get("type") == "table":
                kwargs = component["kwargs"]
                assert "style_header" in kwargs
                assert "style_cell" in kwargs
                assert "style_data_conditional" in kwargs  # Changed from style_data
                assert kwargs["style_header"]["backgroundColor"] == "#3c9639"

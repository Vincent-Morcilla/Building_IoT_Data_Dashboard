import pandas as pd
from helpers.data_processing import apply_transformation


def test_apply_transformation_aggregate():
    """
    Test the aggregation transformation type by grouping data on 'Group'
    and summing 'Value' for each group.
    """
    data = {"Group": ["X", "Y", "X", "Y"], "Value": [10, 20, 30, 40]}
    df = pd.DataFrame(data)
    transformation = {
        "type": "aggregate",
        "groupby": ["Group"],
        "aggregations": {"TotalValue": ("Value", "sum")},
    }
    input_mapping = {}

    result_df = apply_transformation(df, transformation, input_mapping)
    expected_df = pd.DataFrame({"Group": ["X", "Y"], "TotalValue": [40, 60]})

    pd.testing.assert_frame_equal(
        result_df.reset_index(drop=True), expected_df.reset_index(drop=True)
    )


def test_apply_transformation_resample():
    """
    Test the resample transformation type by resampling 'Value' at the
    specified frequency and aggregating by summing values.
    """
    data = {
        "Timestamp": pd.date_range(start="2021-01-01", periods=4, freq="h"),
        "Value": [1, 2, 3, 4],
    }
    df = pd.DataFrame(data)
    transformation = {
        "type": "resample",
        "on": "Timestamp",
        "frequency": "frequency",
        "agg_func": "sum",
    }
    input_mapping = {"frequency": "2h"}

    result_df = apply_transformation(df, transformation, input_mapping)
    expected_df = pd.DataFrame(
        {
            "Timestamp": pd.to_datetime(["2021-01-01 00:00:00", "2021-01-01 02:00:00"]),
            "Value": [3, 7],
        }
    )

    pd.testing.assert_frame_equal(
        result_df.reset_index(drop=True), expected_df.reset_index(drop=True)
    )


def test_apply_transformation_explode():
    """
    Test the explode transformation type by expanding each element in
    'ListValues' into its own row.
    """
    data = {"ID": [1, 2], "ListValues": [[10, 20], [30, 40]]}
    df = pd.DataFrame(data)
    transformation = {"type": "explode", "columns": ["ListValues"]}
    input_mapping = {}

    result_df = apply_transformation(df, transformation, input_mapping)
    expected_df = pd.DataFrame({"ID": [1, 1, 2, 2], "ListValues": [10, 20, 30, 40]})

    pd.testing.assert_frame_equal(
        result_df.reset_index(drop=True),
        expected_df.reset_index(drop=True),
        check_dtype=False,
    )


def test_apply_transformation_invalid_type():
    """
    Test handling of an invalid transformation type by checking that the
    function returns the original DataFrame unchanged.
    """
    data = {"A": [1, 2, 3]}
    df = pd.DataFrame(data)
    transformation = {"type": "unknown"}
    input_mapping = {}

    result_df = apply_transformation(df, transformation, input_mapping)
    expected_df = df.copy()

    pd.testing.assert_frame_equal(
        result_df.reset_index(drop=True), expected_df.reset_index(drop=True)
    )


def test_apply_transformation_explode_missing_column(capsys):
    """
    Test the explode transformation when a specified column is not in the DataFrame.
    """
    data = {"ID": [1, 2], "ListValues": [[10, 20], [30, 40]]}
    df = pd.DataFrame(data)
    transformation = {"type": "explode", "columns": ["NonExistentColumn"]}
    input_mapping = {}

    result_df = apply_transformation(df, transformation, input_mapping)
    expected_df = df.copy()

    pd.testing.assert_frame_equal(
        result_df.reset_index(drop=True),
        expected_df.reset_index(drop=True),
    )

    captured = capsys.readouterr()
    assert (
        "Warning: Column 'NonExistentColumn' not found in DataFrame for exploding."
        in captured.out
    )

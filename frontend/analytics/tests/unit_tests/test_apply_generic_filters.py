import pytest
import pandas as pd
from helpers.data_processing import apply_generic_filters


def test_apply_generic_filters_equals():
    """Test apply_generic_filters with 'equals' condition."""
    data = {"Category": ["A", "B", "A", "C"], "Value": [10, 20, 30, 40]}
    df = pd.DataFrame(data)
    filters = {"Category": {"equals": "selected_category"}}
    input_mapping = {"selected_category": "A"}

    result_df = apply_generic_filters(df, filters, input_mapping)
    expected_df = df[df["Category"] == "A"]

    pd.testing.assert_frame_equal(
        result_df.reset_index(drop=True), expected_df.reset_index(drop=True)
    )


def test_apply_generic_filters_in():
    """Test apply_generic_filters with 'in' condition."""
    data = {"Category": ["A", "B", "C", "D"], "Value": [1, 2, 3, 4]}
    df = pd.DataFrame(data)
    filters = {"Category": {"in": "selected_categories"}}
    input_mapping = {"selected_categories": ["A", "C"]}

    result_df = apply_generic_filters(df, filters, input_mapping)
    expected_df = df[df["Category"].isin(["A", "C"])]

    pd.testing.assert_frame_equal(
        result_df.reset_index(drop=True), expected_df.reset_index(drop=True)
    )


def test_apply_generic_filters_between_dates():
    """Test apply_generic_filters with 'between' condition on dates."""
    data = {
        "Date": pd.date_range(start="2021-01-01", periods=5, freq="D"),
        "Value": [100, 200, 300, 400, 500],
    }
    df = pd.DataFrame(data)
    filters = {
        "Date": {"between": {"start_date": "start_date", "end_date": "end_date"}}
    }
    input_mapping = {
        "start_date": pd.Timestamp("2021-01-02"),
        "end_date": pd.Timestamp("2021-01-04"),
    }

    result_df = apply_generic_filters(df, filters, input_mapping)
    expected_df = df[
        (df["Date"] >= pd.Timestamp("2021-01-02"))
        & (df["Date"] <= pd.Timestamp("2021-01-04"))
    ]

    pd.testing.assert_frame_equal(
        result_df.reset_index(drop=True), expected_df.reset_index(drop=True)
    )


def test_apply_generic_filters_greater_than():
    """Test apply_generic_filters with 'greater_than' condition."""
    data = {"Value": [10, 20, 30, 40, 50]}
    df = pd.DataFrame(data)
    filters = {"Value": {"greater_than": "threshold"}}
    input_mapping = {"threshold": 25}

    result_df = apply_generic_filters(df, filters, input_mapping)
    expected_df = df[df["Value"] > 25]

    pd.testing.assert_frame_equal(
        result_df.reset_index(drop=True), expected_df.reset_index(drop=True)
    )


def test_apply_generic_filters_less_than():
    """Test apply_generic_filters with 'less_than' condition."""
    data = {"Value": [10, 20, 30, 40, 50]}
    df = pd.DataFrame(data)
    filters = {"Value": {"less_than": "threshold"}}
    input_mapping = {"threshold": 35}

    result_df = apply_generic_filters(df, filters, input_mapping)
    expected_df = df[df["Value"] < 35]

    pd.testing.assert_frame_equal(
        result_df.reset_index(drop=True), expected_df.reset_index(drop=True)
    )


def test_apply_generic_filters_nonexistent_column():
    """Test apply_generic_filters when the filter column does not exist."""
    data = {"A": [1, 2, 3]}
    df = pd.DataFrame(data)
    filters = {"B": {"equals": "value"}}
    input_mapping = {"value": 2}

    # Since 'B' doesn't exist, the DataFrame should remain unchanged
    result_df = apply_generic_filters(df, filters, input_mapping)
    expected_df = df.copy()

    pd.testing.assert_frame_equal(
        result_df.reset_index(drop=True), expected_df.reset_index(drop=True)
    )

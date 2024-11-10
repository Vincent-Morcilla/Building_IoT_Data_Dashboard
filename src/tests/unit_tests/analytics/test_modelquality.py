"""Unit tests for the modelquality module in the analytics package."""

import pandas as pd
import pytest

import analytics.modules.modelquality as mq

# Suppress pylint warnings for fixtures
# pylint: disable=protected-access
# pylint: disable=redefined-outer-name


def test_generate_id():
    """Unit test for the _generate_id function in the modelquality module."""
    # Test case with no suffix
    result = mq._generate_id("category", "subcategory", "component")
    assert result == "category-subcategory-component"

    # Test case with suffix
    result = mq._generate_id("category", "subcategory", "component", "suffix")
    assert result == "category-subcategory-component-suffix"

    # Test case with non-string input
    result = mq._generate_id("Category", "SubCategory", "Component", 123)
    assert result == "category-subcategory-component-123"


def test_generate_green_scale_colour_map():
    """
    Unit test for the _generate_green_scale_colour_map function in the
    modelquality module.
    """
    # Test with one label
    labels = ["label1"]
    result = mq._generate_green_scale_colour_map(labels)
    assert len(result) == 1
    assert result["label1"] == "#3c9639"  # The theme green

    # Test with two labels
    labels = ["label1", "label2"]
    result = mq._generate_green_scale_colour_map(labels)
    assert len(result) == 2
    assert result["label1"] == "#3c9639"
    assert result["label2"] == "#2d722c"  # Another green shade

    # Test with more than two labels
    labels = ["label1", "label2", "label3"]
    result = mq._generate_green_scale_colour_map(labels)
    assert len(result) == 3
    assert result["label1"] != result["label2"]  # Different colors


@pytest.fixture
def sample_df():
    """Fixture to provide a sample DataFrame."""
    return pd.DataFrame({"label": ["A", "B", "C"], "value": [10, 20, 30]})


def test_build_pie_chart_component(sample_df):
    """
    Unit test for the _build_pie_chart_component function in the modelquality
    module using a sample DataFrame.
    """
    title = "Test Pie Chart"
    component_id = "test-pie-chart"
    label = "label"

    result = mq._build_pie_chart_component(title, component_id, sample_df, label)

    assert result["type"] == "plot"
    assert result["library"] == "go"
    assert result["trace_type"] == "Pie"
    assert "marker" in result["kwargs"]
    assert "colors" in result["kwargs"]["marker"]
    assert result["layout_kwargs"]["title"]["text"] == title
    assert result["layout_kwargs"]["title"]["xanchor"] == "center"


@pytest.fixture
def sample_table_df():
    """Fixture to provide a sample DataFrame for a table component."""
    return pd.DataFrame({"column1": [1, 2, 3], "column2": [4, 5, 6]})


def test_build_table_component(sample_table_df):
    """
    Unit test for the _build_table_component function in the modelquality module
    using a sample DataFrame.
    """
    title = "Test Table"
    component_id = "test-table"
    columns = ["column1", "column2"]

    result = mq._build_table_component(title, component_id, sample_table_df, columns)

    assert result["type"] == "table"
    assert result["dataframe"] is sample_table_df
    assert result["id"] == component_id
    assert result["title"] == title
    assert result["kwargs"]["columns"] == columns
    assert result["kwargs"]["export_format"] == "csv"

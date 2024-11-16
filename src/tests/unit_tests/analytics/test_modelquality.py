"""Unit tests for the modelquality module in the analytics package."""

from unittest.mock import MagicMock

import pandas as pd
import pytest
from rdflib import URIRef

from analytics.dbmgr import DBManager
import analytics.modules.modelquality as mq

# Suppress pylint warnings for fixtures
# pylint: disable=protected-access
# pylint: disable=redefined-outer-name
# pylint: disable=singleton-comparison


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


def test_missing_labels_in_pie_chart():
    """
    Unit test for the _build_pie_chart_component function in the modelquality
    module with a DataFrame missing the label column.
    """
    # Missing label column in the dataframe
    sample_df = pd.DataFrame({"value": [10, 20, 30]})
    with pytest.raises(KeyError):
        mq._build_pie_chart_component(
            "Test Pie Chart", "test-pie-chart", sample_df, "label"
        )


@pytest.fixture
def mock_db_manager():
    """Fixture to provide a mock Database Manager."""
    mock_db = MagicMock(spec=DBManager)

    # When query is called, return a DataFrame with mock data for 3 entities
    mock_db.query.return_value = pd.DataFrame(
        [
            {
                "entity_id": "e1",
                "brick_class": URIRef("brick#ClassA"),
                "stream_id": "s1",
                "named_unit": URIRef("unit#KWh"),
                "anonymous_unit": None,
            },
            {
                "entity_id": "e2",
                "brick_class": URIRef("brick#ClassB"),
                "stream_id": "s2",
                "named_unit": None,
                "anonymous_unit": "MWh",
            },
            {
                "entity_id": "e3",
                "brick_class": URIRef("brick#ClassC"),
                "stream_id": "s3",
                "named_unit": None,
                "anonymous_unit": None,
            },
        ]
    )

    # Mock the schema to include only two of the above classes
    mock_db.schema = {
        (URIRef("brick#ClassA"), None, None),
        (URIRef("brick#ClassC"), None, None),
    }

    # Mock the mapper to include only two of the above streams
    mock_db.mapper = pd.DataFrame(
        {"StreamID": ["s1", "s2"], "strBrickLabel": ["ClassA", "NotClassB"]}
    )

    # Patch defrag_uri to return a URIRef-like object
    def fake_defrag_uri(uri):
        class URIRefMock:
            """Class to mock the URIRef object."""

            def __init__(self, uri):
                if isinstance(uri, URIRef):
                    if "#" in uri:
                        self.fragment = uri.fragment
                    elif "/" in uri:
                        self.fragment = uri.split("/")[-1]
                else:
                    self.fragment = uri

            def __bool__(self):
                return self.fragment != None

            def __eq__(self, other):
                return self.fragment == other

            def __hash__(self):
                return hash(self.fragment)

            def __lt__(self, other):
                if self.fragment == None:
                    return False
                if other == None:
                    return True
                return self.fragment < other.fragment

            def __str__(self):
                return str(self.fragment)

        return URIRefMock(uri)

    mock_db.defrag_uri = fake_defrag_uri

    return mock_db


def test_build_master_df(mock_db_manager):
    """
    Unit test for the _build_master_df function in the modelquality module using
    a mock Database Manager.
    """
    # Run the function under test
    df = mq._build_master_df(mock_db_manager)

    # Assertions
    assert df is not None
    assert len(df) == 3

    assert "entity_id" in df.columns
    assert df["entity_id"].iloc[0] == "e1"
    assert df["entity_id"].iloc[1] == "e2"
    assert df["entity_id"].iloc[2] == "e3"

    assert "brick_class" in df.columns
    assert df["brick_class"].iloc[0] == "ClassA"  # Fragment of the URIRef
    assert df["brick_class"].iloc[1] == "ClassB"  # Fragment of the URIRef
    assert df["brick_class"].iloc[2] == "ClassC"  # Fragment of the URIRef

    assert "stream_id" in df.columns
    assert df["stream_id"].iloc[0] == "s1"
    assert df["stream_id"].iloc[1] == "s2"
    assert df["stream_id"].iloc[2] == "s3"

    assert "named_unit" in df.columns
    assert df["named_unit"].iloc[0] == "KWh"  # Fragment of the URIRef
    assert df["named_unit"].iloc[1] == None
    assert df["named_unit"].iloc[2] == None

    assert "anonymous_unit" in df.columns
    assert df["anonymous_unit"].iloc[0] == None
    assert df["anonymous_unit"].iloc[1] == "MWh"
    assert df["anonymous_unit"].iloc[2] == None

    assert "class_in_brick_schema" in df.columns
    assert df["class_in_brick_schema"].iloc[0] == "Recognised"
    assert df["class_in_brick_schema"].iloc[1] == "Unrecognised"
    assert df["class_in_brick_schema"].iloc[2] == "Recognised"

    assert "unit" in df.columns
    assert df["unit"].iloc[0] == "KWh"
    assert df["unit"].iloc[1] == "MWh"
    assert df["unit"].iloc[2] == None

    assert "unit_is_named" in df.columns
    assert df["unit_is_named"].iloc[0] == True
    assert df["unit_is_named"].iloc[1] == False
    assert df["unit_is_named"].iloc[2] == None

    assert "stream_exists_in_mapping" in df.columns
    assert df["stream_exists_in_mapping"].iloc[0] == True
    assert df["stream_exists_in_mapping"].iloc[1] == True
    assert df["stream_exists_in_mapping"].iloc[2] == False

    assert "brick_class_in_mapper" in df.columns
    assert df["brick_class_in_mapper"].iloc[0] == "ClassA"
    assert df["brick_class_in_mapper"].iloc[1] == "NotClassB"
    assert df["brick_class_in_mapper"].iloc[2] == None

    assert "brick_class_is_consistent" in df.columns
    assert df["brick_class_is_consistent"].iloc[0] == True
    assert df["brick_class_is_consistent"].iloc[1] == False
    assert df["brick_class_is_consistent"].iloc[2] == None


def test_build_master_df_with_empty_query(mocker):
    """
    Unit test for the _build_master_df function in the modelquality module
    where querying the database produces an empty response.
    """
    mock_db = mocker.Mock(spec=DBManager)
    mock_db.query.return_value = pd.DataFrame()

    df = mq._build_master_df(mock_db)

    assert len(df) == 0


def test_build_master_df_with_missing_stream_id(mock_db_manager):
    """
    Unit test for the _build_master_df function in the modelquality module
    where the stream_id is missing in the database response.
    """
    # Modify the query return value for this specific test
    mock_db_manager.query.return_value = pd.DataFrame(
        [
            {
                "entity_id": "e4",
                "brick_class": URIRef("brick#ClassD"),
                "stream_id": None,
                "named_unit": None,
                "anonymous_unit": None,
            }
        ]
    )

    # Call the function with the modified mock
    df = mq._build_master_df(mock_db_manager)

    # Assertions
    assert len(df) == 1
    assert "stream_id" in df.columns
    assert df["stream_id"].iloc[0] == None
    assert df["entity_id"].iloc[0] == "e4"
    assert df["brick_class"].iloc[0] == "ClassD"


def test_build_master_df_with_no_named_unit(mock_db_manager):
    """
    Unit test for the _build_master_df function in the modelquality module
    where the there is no named_unit column in the database response.
    """
    # Modify the query return value for this specific test
    mock_db_manager.query.return_value = pd.DataFrame(
        [
            {
                "entity_id": "e5",
                "brick_class": URIRef("brick#ClassE"),
                "stream_id": "s5",
                "anonymous_unit": None,
            }
        ]
    )

    # Call the function with the modified mock
    df = mq._build_master_df(mock_db_manager)

    # Assertions
    assert len(df) == 1
    assert "named_unit" in df.columns


def test_build_master_df_with_no_anomymous_unit(mock_db_manager):
    """
    Unit test for the _build_master_df function in the modelquality module
    where the there is no anonymous_unit column in the database response.
    """
    # Modify the query return value for this specific test
    mock_db_manager.query.return_value = pd.DataFrame(
        [
            {
                "entity_id": "e6",
                "brick_class": URIRef("brick#ClassF"),
                "stream_id": "s6",
                "named_unit": None,
            }
        ]
    )

    # Call the function with the modified mock
    df = mq._build_master_df(mock_db_manager)

    # Assertions
    assert len(df) == 1
    assert "anonymous_unit" in df.columns


def test_recognised_entity_analysis(mock_db_manager):
    """
    Unit test for the _recognised_entity_analysis function in the modelquality
    module using a mock Database Manager.
    """
    # Set up the test
    df = mq._build_master_df(mock_db_manager)

    # Run the function under test
    result = mq._recognised_entity_analysis(df)

    category = ("ModelQuality", "RecognisedEntities")
    # Check that result contains correct keys
    assert category in result
    assert "title" in result[category]

    components = result[category]["components"]
    assert len(components) > 0  # Check that components list is populated

    # Check for pie chart and table components
    pie_chart_1 = [comp for comp in components if "pie-1" in comp["id"]]
    assert len(pie_chart_1) > 0

    table_config = [comp for comp in components if "table" in comp["id"]]
    assert len(table_config) > 0


def test_associated_units_analysis(mock_db_manager):
    """
    Unit test for the _associated_units_analysis function in the modelquality
    module using a mock Database Manager.
    """
    # Set up the test
    df = mq._build_master_df(mock_db_manager)

    # Run the function under test
    result = mq._associated_units_analysis(df)

    category = ("ModelQuality", "AssociatedUnits")
    # Check that result contains correct keys
    assert category in result
    assert "title" in result[category]

    components = result[category]["components"]
    assert len(components) > 0  # Check that components list is populated

    # Check for pie chart and table components
    pie_chart_1 = [comp for comp in components if "pie-1" in comp["id"]]
    assert len(pie_chart_1) > 0

    table_config = [comp for comp in components if "table" in comp["id"]]
    assert len(table_config) > 0


def test_associated_timeseries_data_analysis(mock_db_manager):
    """
    Unit test for the _associated_timeseries_data_analysis function in the
    modelquality module using a mock Database Manager.
    """
    # Set up the test
    df = mq._build_master_df(mock_db_manager)

    # Run the function under test
    result = mq._associated_timeseries_data_analysis(df)

    category = ("ModelQuality", "TimeseriesData")
    # Check that result contains correct keys
    assert category in result
    assert "title" in result[category]

    components = result[category]["components"]
    assert len(components) > 0  # Check that components list is populated

    # Check for pie chart and table components
    pie_chart_1 = [comp for comp in components if "pie-1" in comp["id"]]
    assert len(pie_chart_1) > 0

    table_config = [comp for comp in components if "table" in comp["id"]]
    assert len(table_config) > 0


def test_class_consistency_analysis(mock_db_manager):
    """
    Unit test for the _class_consistency_analysis function in the modelquality
    module using a mock Database Manager.
    """
    # Set up the test
    df = mq._build_master_df(mock_db_manager)

    # Run the function under test
    result = mq._class_consistency_analysis(df)

    category = ("ModelQuality", "ClassConsistency")
    # Check that result contains correct keys
    assert category in result
    assert "title" in result[category]

    components = result[category]["components"]
    assert len(components) > 0  # Check that components list is populated

    # Check for pie chart and table components
    pie_chart_1 = [comp for comp in components if "pie-1" in comp["id"]]
    assert len(pie_chart_1) > 0

    table_config = [comp for comp in components if "table" in comp["id"]]
    assert len(table_config) > 0


def test_run_with_empty_db(mocker):
    """
    Unit test for the run function in the modelquality module with an empty
    database response.
    """
    mock_db = mocker.Mock(spec=DBManager)

    # Mock _build_master_df to return an empty DataFrame
    mocker.patch(
        "analytics.modules.modelquality._build_master_df",
        return_value=pd.DataFrame(),
    )

    result = mq.run(mock_db)

    assert ("ModelQuality", "Error") in result
    assert "components" in result[("ModelQuality", "Error")]
    assert len(result[("ModelQuality", "Error")]["components"]) == 1
    assert "type" in result[("ModelQuality", "Error")]["components"][0]
    assert result[("ModelQuality", "Error")]["components"][0]["type"] == "error"


def test_run_with_sample_data(mocker, mock_db_manager):
    """
    Unit test for the run function in the modelquality module with a database
    response containing data.
    """
    # Mock _build_master_df to return a sample DataFrame
    mocker.patch(
        "analytics.modules.modelquality._build_master_df",
        return_value=pd.DataFrame(
            {
                "entity_id": ["e1", "e2", "e3"],
                "brick_class": ["ClassA", "ClassB", "ClassC"],
                "stream_id": ["s1", "s2", "s3"],
                "named_unit": ["KWh", None, None],
                "anonymous_unit": [None, "MWh", None],
                "class_in_brick_schema": ["Recognised", "Unrecognised", "Recognised"],
                "unit": ["KWh", "MWh", None],
                "unit_is_named": [True, False, None],
                "stream_exists_in_mapping": [True, True, False],
                "brick_class_in_mapper": ["ClassA", "NotClassB", None],
                "brick_class_is_consistent": [True, False, None],
            }
        ),
    )

    # Run the function under test
    result = mq.run(mock_db_manager)

    # Assertions
    assert result is not None
    assert len(result) > 0
    assert isinstance(result, dict)

    # Check the keys in the result
    assert ("ModelQuality", "RecognisedEntities") in result
    assert ("ModelQuality", "AssociatedUnits") in result
    assert ("ModelQuality", "TimeseriesData") in result
    assert ("ModelQuality", "ClassConsistency") in result

    # Check the components in the result
    assert len(result[("ModelQuality", "RecognisedEntities")]["components"]) > 0
    assert len(result[("ModelQuality", "AssociatedUnits")]["components"]) > 0
    assert len(result[("ModelQuality", "TimeseriesData")]["components"]) > 0
    assert len(result[("ModelQuality", "ClassConsistency")]["components"]) > 0


def test_run_with_mock_db(mock_db_manager):
    """
    Unit test for the run function in the modelquality module with a mock
    Database Manager.
    """
    # Run the function under test
    result = mq.run(mock_db_manager)

    # Assertions
    assert result is not None
    assert len(result) > 0
    assert isinstance(result, dict)

    # Check the keys in the result
    assert ("ModelQuality", "RecognisedEntities") in result
    assert ("ModelQuality", "AssociatedUnits") in result
    assert ("ModelQuality", "TimeseriesData") in result
    assert ("ModelQuality", "ClassConsistency") in result

    # Check the components in the result
    assert len(result[("ModelQuality", "RecognisedEntities")]["components"]) > 0
    assert len(result[("ModelQuality", "AssociatedUnits")]["components"]) > 0
    assert len(result[("ModelQuality", "TimeseriesData")]["components"]) > 0
    assert len(result[("ModelQuality", "ClassConsistency")]["components"]) > 0

"""Unit tests for the modelquality module in the analytics package."""

from unittest.mock import MagicMock, patch
import pandas as pd
import pytest
from rdflib import URIRef

from analytics.dbmgr import DBManager
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


def test_missing_labels_in_pie_chart():
    # Missing label column in the dataframe
    sample_df = pd.DataFrame({"value": [10, 20, 30]})
    with pytest.raises(KeyError):
        mq._build_pie_chart_component(
            "Test Pie Chart", "test-pie-chart", sample_df, "label"
        )


# # Define the mock_db_manager fixture
# @pytest.fixture
# def mock_db_manager():
#     # Create a MagicMock for the DBManager class
#     mock_db_manager = MagicMock(spec=DBManager)

#     # Create mock data with expected columns and types
#     mock_data = {
#         "Building": ["Building1", "Building2", "Building3"],
#         "StreamID": [1, 2, 3],
#         "Filename": ["file1", "file2", "file3"],
#         "strBrickLabel": ["label1", "label2", "label3"],
#         "brick_class": [
#             URIRef("http://example.org/class1"),
#             URIRef("http://example.org/class2"),
#             URIRef("http://example.org/class3"),
#         ],
#     }

#     # Convert mock data into a DataFrame
#     mock_df = pd.DataFrame(mock_data)

#     # Mock the mapper to return the mock data when called
#     mock_db_manager.mapper.__getitem__.return_value = mock_df

#     return mock_db_manager


@pytest.fixture
def mock_db_manager():
    """Fixture to provide a mock Database Manager."""
    mock_db = MagicMock(spec=DBManager)
    mock_db.query.return_value = pd.DataFrame(
        [
            {
                "entity_id": URIRef("brick#e1"),
                "brick_class": URIRef("brick#ClassA"),
                "stream_id": URIRef("brick#s1"),
                "named_unit": URIRef("unit#KWh"),
                # "anonymous_unit": URIRef("unit#KWh"),
                "anonymous_unit": None,
            },
            {
                "entity_id": URIRef("brick#e2"),
                "brick_class": URIRef("brick#ClassB"),
                "stream_id": URIRef("brick#s2"),
                # "named_unit": URIRef("unit#MWh"),
                "named_unit": None,
                # "anonymous_unit": URIRef("unit#MWh"),
                "anonymous_unit": "MWh",
            },
            # {
            #     "entity_id": "e2",
            #     "brick_class": "brick:ClassB",
            #     "stream_id": "s2",
            #     "named_unit": None,
            #     "anonymous_unit": "unit:MWh",
            # },
        ]
    )
    mock_db.schema = {
        (URIRef("brick:ClassA"), None, None),
        (URIRef("brick:ClassB"), None, None),
    }
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

            def __eq__(self, other):
                return self.fragment == other

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
    assert len(df) == 2

    assert "entity_id" in df.columns
    assert df["entity_id"].iloc[0] == "e1"  # Fragment of the URIRef
    assert df["entity_id"].iloc[1] == "e2"  # Fragment of the URIRef

    assert "brick_class" in df.columns
    assert df["brick_class"].iloc[0] == "ClassA"  # Fragment of the URIRef
    assert df["brick_class"].iloc[1] == "ClassB"  # Fragment of the URIRef

    assert "stream_id" in df.columns
    assert df["stream_id"].iloc[0] == "s1"  # Fragment of the URIRef
    assert df["stream_id"].iloc[1] == "s2"  # Fragment of the URIRef


# # Mock Data for Testing
# @pytest.fixture
# def mock_db_manager():
#     db_mock = MagicMock(spec=DBManager)
#     db_mock.query.return_value = pd.DataFrame(
#         {
#             "brick_class": ["Class1", "Class2", "Class1"],
#             "entity_id": ["E1", "E2", "E3"],
#             "class_in_brick_schema": ["Recognised", "Unrecognised", "Recognised"],
#             "stream_id": ["S1", "S2", "S3"],
#             "unit": [None, "Unit1", "Unit2"],
#             "unit_is_named": [True, False, True],
#             "stream_exists_in_mapping": [True, False, True],
#             "brick_class_in_mapper": ["Class1", "Class2", "Class1"],
#             "brick_class_is_consistent": [True, False, True],
#         }
#     )
#     return db_mock


# def test_recognised_entity_analysis(mock_db_manager):
#     # Test _recognised_entity_analysis function
#     df = mq._build_master_df(mock_db_manager)
#     result = mq._recognised_entity_analysis(df)

#     # Check that result contains correct keys
#     assert "RecognisedEntities" in result
#     assert "title" in result[("RecognisedEntities")]

#     components = result[("RecognisedEntities")]["components"]
#     assert len(components) > 0  # Check that components list is populated

#     # Check for pie chart and table components
#     pie_chart_1 = [comp for comp in components if "pie-1" in comp["id"]]
#     assert len(pie_chart_1) > 0

#     table_config = [comp for comp in components if "table" in comp["id"]]
#     assert len(table_config) > 0

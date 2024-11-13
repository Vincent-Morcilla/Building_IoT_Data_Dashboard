"""Unit tests for the DBManager class in the analytics.dbmgr module."""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
import rdflib


from analytics.dbmgr import DBManager
from analytics.dbmgr import DBManagerFileNotFoundError
from analytics.dbmgr import DBManagerBadCsvFile
from analytics.dbmgr import DBManagerBadRdfFile

# Suppress pylint warnings for fixtures
# pylint: disable=protected-access
# pylint: disable=redefined-outer-name
# pylint: disable=unused-argument

# Sample file paths for testing
DATA_ZIP_PATH = "data.zip"
MAPPER_PATH = "mapper.csv"
MODEL_PATH = "model.rdf"
SCHEMA_PATH = "schema.rdf"
BUILDING = "BuildingA"

# Sample data for mocking
sample_mapper_data = pd.DataFrame(
    {
        "StreamID": ["stream1", "stream2", "stream3", "stream4"],
        "Filename": ["file1.pkl", "file2.pkl", "file3.pkl", "FILE NOT SAVED"],
        "Building": [BUILDING, "BuildingB", BUILDING, BUILDING],
        "strBrickLabel": ["Temperature", "Temperature", "Temperature", "Temperature"],
    }
)

sample_stream_data = pd.DataFrame(
    {
        "t": ["2024-01-01T00:00:00", "2024-01-01T01:00:00"],
        "v": [10, 20],
        "y": ["Temperature", "Temperature"],
    }
)


@pytest.fixture
def disable_tqdm():
    """Fixture to disable tqdm progress bars."""
    with patch("analytics.dbmgr.tqdm", lambda x, *args, **kwargs: x):
        yield


@pytest.fixture
def db_manager_notfound_files(request, disable_tqdm):
    """Fixture to provide a DBManager instance with sample data."""
    file_existence = request.param  # Dictionary controlling individual file existence

    # Define a helper function to mock `Path.is_file` based on file_existence
    def mock_is_file(path):
        return file_existence.get(str(path), False)  # Default to False if not specified

    with patch("pathlib.Path.is_file", new=mock_is_file):
        # If any critical file is notfound, avoid creating DBManager and yield None
        if not all(
            file_existence.get(f, True)
            for f in ["data.zip", MAPPER_PATH, MODEL_PATH, SCHEMA_PATH]
        ):
            yield None
            return

        # Patch pandas read_csv to return the sample mapper data
        with patch("analytics.dbmgr.pd.read_csv", return_value=sample_mapper_data):
            # Mock zip file operations
            with patch("analytics.dbmgr.zipfile.ZipFile") as mock_zip:
                mock_zip.return_value.__enter__.return_value.namelist.return_value = [
                    "file1.pkl",
                    "file2.pkl",
                    "file3.pkl",
                ]
                # Patch pickle.loads to return the sample stream data
                with patch(
                    "analytics.dbmgr.pickle.loads",
                    return_value=sample_stream_data.to_dict(),
                ):
                    # Mock loading of brickschema.Graph files
                    with patch(
                        "analytics.dbmgr.brickschema.Graph.load_file"
                    ) as mock_load_file:
                        mock_load_file.return_value = MagicMock()
                        # Create the DBManager instance
                        yield DBManager(
                            data_zip_path=DATA_ZIP_PATH,
                            mapper_path=MAPPER_PATH,
                            model_path=MODEL_PATH,
                            schema_path=SCHEMA_PATH,
                            building=BUILDING,
                        )


# Parameterize with individual file existence combinations
@pytest.mark.parametrize(
    "db_manager_notfound_files",
    [
        {
            DATA_ZIP_PATH: True,
            MAPPER_PATH: True,
            MODEL_PATH: True,
            SCHEMA_PATH: True,
        },  # All files exist
        {
            DATA_ZIP_PATH: False,
            MAPPER_PATH: True,
            MODEL_PATH: True,
            SCHEMA_PATH: True,
        },  # data.zip notfound
        {
            DATA_ZIP_PATH: True,
            MAPPER_PATH: False,
            MODEL_PATH: True,
            SCHEMA_PATH: True,
        },  # mapper.csv notfound
        {
            DATA_ZIP_PATH: True,
            MAPPER_PATH: True,
            MODEL_PATH: False,
            SCHEMA_PATH: True,
        },  # model.rdf notfound
        {
            DATA_ZIP_PATH: True,
            MAPPER_PATH: True,
            MODEL_PATH: True,
            SCHEMA_PATH: False,
        },  # schema.rdf notfound
        {
            DATA_ZIP_PATH: False,
            MAPPER_PATH: False,
            MODEL_PATH: False,
            SCHEMA_PATH: False,
        },  # No files exist
    ],
    indirect=True,
)
def test_db_manager_individual_file_existence(db_manager_notfound_files):
    """Test DBManager behavior with various combinations of notfound files."""
    if db_manager_notfound_files is None:
        # Expect DBManagerFileNotFoundError when any critical file is notfound
        with pytest.raises(DBManagerFileNotFoundError):
            DBManager(
                data_zip_path=DATA_ZIP_PATH,
                mapper_path=MAPPER_PATH,
                model_path=MODEL_PATH,
                schema_path=SCHEMA_PATH,
                building=BUILDING,
            )
    else:
        # All files exist, we should have a valid DBManager instance
        assert db_manager_notfound_files is not None


def test_db_manager_bad_csv_file():
    """
    Test that DBManager raises DBManagerBadCsvFile if there is a CSV parsing
    error.
    """
    with patch("pathlib.Path.is_file", return_value=True), patch(
        "analytics.dbmgr.pd.read_csv", side_effect=pd.errors.ParserError
    ):
        with pytest.raises(DBManagerBadCsvFile, match="Error reading CSV file"):
            DBManager(
                data_zip_path=DATA_ZIP_PATH,
                mapper_path=MAPPER_PATH,
                model_path=MODEL_PATH,
                schema_path=SCHEMA_PATH,
                building=BUILDING,
            )


def test_db_manager_bad_rdf_file_model():
    """
    Test that DBManager raises DBManagerBadRdfFile if there is an error loading
    the model RDF file.
    """
    with patch("pathlib.Path.is_file", return_value=True), patch(
        "analytics.dbmgr.pd.read_csv", return_value=pd.DataFrame()
    ), patch("analytics.dbmgr.brickschema.Graph.load_file", side_effect=AttributeError):
        with pytest.raises(DBManagerBadRdfFile, match="Error reading RDF file"):
            DBManager(
                data_zip_path=DATA_ZIP_PATH,
                mapper_path=MAPPER_PATH,
                model_path=MODEL_PATH,
                schema_path=SCHEMA_PATH,
                building=BUILDING,
            )


def test_db_manager_bad_rdf_file_schema():
    """
    Test that DBManager raises DBManagerBadRdfFile if there is an error loading
    the schema RDF file.
    """
    with patch("pathlib.Path.is_file", return_value=True), patch(
        "analytics.dbmgr.pd.read_csv", return_value=pd.DataFrame()
    ), patch(
        "analytics.dbmgr.brickschema.Graph.load_file",
        side_effect=[MagicMock(), AttributeError],
    ):
        with pytest.raises(DBManagerBadRdfFile, match="Error reading RDF file"):
            DBManager(
                data_zip_path=DATA_ZIP_PATH,
                mapper_path=MAPPER_PATH,
                model_path=MODEL_PATH,
                schema_path=SCHEMA_PATH,
                building=BUILDING,
            )


@pytest.fixture
def db_manager(disable_tqdm):
    """Fixture to provide a DBManager instance with sample data."""
    # Patch all Path existence checks to avoid FileNotFoundError
    with patch("pathlib.Path.is_file", return_value=True):
        # Patch pandas read_csv to return the sample mapper data
        with patch("analytics.dbmgr.pd.read_csv", return_value=sample_mapper_data):
            # Mock zip file operations
            with patch("analytics.dbmgr.zipfile.ZipFile") as mock_zip:
                mock_zip.return_value.__enter__.return_value.namelist.return_value = [
                    "file1.pkl",
                    "file2.pkl",
                    "file3.pkl",
                ]
                # Patch pickle.loads to return the sample stream data
                with patch(
                    "analytics.dbmgr.pickle.loads",
                    return_value=sample_stream_data.to_dict(),
                ):
                    # Mock loading of brickschema.Graph files
                    with patch(
                        "analytics.dbmgr.brickschema.Graph.load_file"
                    ) as mock_load_file:
                        mock_load_file.return_value = MagicMock()
                        # Create the DBManager instance
                        return DBManager(
                            data_zip_path=DATA_ZIP_PATH,
                            mapper_path=MAPPER_PATH,
                            model_path=MODEL_PATH,
                            schema_path=SCHEMA_PATH,
                            building=BUILDING,
                        )


def test_initialisation(db_manager):
    """Test DBManager initialisation with the db_manager fixture."""
    # Assertions to confirm initialization paths and data
    assert str(db_manager._data_zip_path) == DATA_ZIP_PATH
    assert str(db_manager._mapper_path) == MAPPER_PATH
    assert str(db_manager._model_path) == MODEL_PATH
    assert str(db_manager._schema_path) == SCHEMA_PATH
    assert "stream1" in db_manager.mapper["StreamID"].values
    assert BUILDING in db_manager.mapper["Building"].values


def test_len(db_manager):
    """Test the __len__ method of the DBManager class."""
    assert len(db_manager) == 2  # Based on sample_stream_data


def test_iter(db_manager):
    """Test the __iter__ method of the DBManager class."""
    assert list(iter(db_manager)) == ["stream1", "stream3"]


def test_contains(db_manager):
    """Test the __contains__ method of the DBManager class."""
    assert "stream1" in db_manager
    assert "stream2" not in db_manager
    assert "stream3" in db_manager
    assert "stream4" not in db_manager


def test_getitem(db_manager):
    """Test the __getitem__ method of the DBManager class."""
    stream_data = db_manager["stream1"]
    assert isinstance(stream_data, pd.DataFrame)
    assert "time" in stream_data.columns
    assert "value" in stream_data.columns
    assert "brick_class" in stream_data.columns


def test_getitem_keyerror(db_manager):
    """Test the __getitem__ method of the DBManager class with a KeyError."""
    with pytest.raises(KeyError):
        _ = db_manager["stream2"]


def test_setitem(db_manager):
    """Test the __setitem__ method of the DBManager class."""
    new_data = pd.DataFrame(
        {
            "time": ["2024-01-01T00:00:00", "2024-01-01T01:00:00"],
            "value": [40, 50],
            "brick_class": ["Humidity", "Humidity"],
        }
    )
    db_manager["new_stream"] = new_data
    assert db_manager["new_stream"].equals(new_data)


def test_data_property(db_manager):
    """Test the data property of the DBManager class."""
    assert isinstance(db_manager.data, dict)
    assert "stream1" in db_manager.data


def test_mapper_property(db_manager):
    """Test the mapper property of the DBManager class."""
    assert isinstance(db_manager.mapper, pd.DataFrame)
    assert "StreamID" in db_manager.mapper.columns


def test_model_property(db_manager):
    """Test the model property of the DBManager class."""
    assert isinstance(db_manager.model, MagicMock)


def test_schema_property(db_manager):
    """Test the schema property of the DBManager class."""
    assert isinstance(db_manager.schema, MagicMock)


def test_schema_and_model_property(db_manager):
    """Test the schema_and_model property of the DBManager class."""
    assert isinstance(db_manager.schema_and_model, MagicMock)


def test_expanded_model_property(db_manager):
    """Test the expanded_model property of the DBManager class."""
    assert isinstance(db_manager.expanded_model, MagicMock)


def test_get_stream(db_manager):
    """Test the get_stream method of the DBManager class."""
    stream_data = db_manager.get_stream("stream1")
    assert isinstance(stream_data, pd.DataFrame)
    assert "time" in stream_data.columns
    assert "value" in stream_data.columns
    assert "brick_class" in stream_data.columns


def test_get_stream_keyerror(db_manager):
    """Test the get_stream method of the DBManager class with a KeyError."""
    with pytest.raises(KeyError, match="Stream ID 'stream2' not found in the database"):
        _ = db_manager.get_stream("stream2")


def test_set_stream(db_manager):
    """Test the set_stream method of the DBManager class."""
    new_data = pd.DataFrame(
        {
            "time": ["2024-01-01T00:00:00", "2024-01-01T01:00:00"],
            "value": [400, 500],
            "brick_class": ["Pressure", "Pressure"],
        }
    )
    db_manager.set_stream("stream3", new_data)
    assert db_manager["stream3"].equals(new_data)


def test_get_streams(db_manager):
    """Test the get_streams method of the DBManager class."""
    streams = db_manager.get_streams(["stream1", "stream3"])
    assert isinstance(streams, dict)
    assert "stream1" in streams
    assert isinstance(streams["stream1"], pd.DataFrame)
    assert "stream3" in streams
    assert isinstance(streams["stream3"], pd.DataFrame)


def test_set_streams(db_manager):
    """Test the set_streams method of the DBManager class."""
    new_data = {
        "stream3": pd.DataFrame(
            {
                "time": ["2024-01-01T00:00:00", "2024-01-01T01:00:00"],
                "value": [700, 800],
                "brick_class": ["Light", "Light"],
            }
        )
    }
    db_manager.set_streams(new_data)
    assert "stream3" in db_manager
    assert db_manager["stream3"].equals(new_data["stream3"])


def test_get_all_streams(db_manager):
    """Test the get_all_streams method of the DBManager class."""
    streams = db_manager.get_all_streams()
    assert isinstance(streams, dict)
    assert "stream1" in streams
    assert isinstance(streams["stream1"], pd.DataFrame)
    assert "stream3" in streams
    assert isinstance(streams["stream3"], pd.DataFrame)


def test_get_stream_label(db_manager):
    """Test the get_stream_label method of the DBManager class."""
    label = db_manager.get_stream_label("stream1")
    assert label == "Temperature"


def test_get_stream_label_keyerror(db_manager):
    """Test the get_stream_label method of the DBManager class with a KeyError."""
    with pytest.raises(KeyError, match="Stream ID 'stream2' not found in the database"):
        _ = db_manager.get_stream_label("stream2")


def test_get_label(db_manager):
    """Test the get_label method of the DBManager class."""
    label = db_manager.get_label("stream1")
    assert label == "Temperature"


def test_get_label_keyerror(db_manager):
    """Test the get_stream_label method of the DBManager class with a KeyError."""
    with pytest.raises(KeyError, match="Stream ID 'stream2' not found in the database"):
        _ = db_manager.get_label("stream2")


def test_query(db_manager):
    """Test the query method of the DBManager class."""
    mock_graph = MagicMock()
    mock_graph.query.return_value = [("result1",), ("result2",)]
    db_manager._g["model"] = mock_graph
    results = db_manager.query("SELECT * WHERE { ?s ?p ?o }")
    assert len(results) == 2


def test_query_return_df(db_manager):
    """Test the query method of the DBManager class when returning a Dataframe."""
    # Sample SPARQL query string and expected mock data
    query_str = "SELECT ?s ?p ?o WHERE { ?s ?p ?o . }"
    mock_bindings = [
        {"s": "subject1", "p": "predicate1", "o": "object1"},
        {"s": "subject2", "p": "predicate2", "o": "object2"},
    ]

    # Patch the query method of the mock graph to return the mocked results
    with patch.object(db_manager, "_g", {"model": MagicMock()}):
        mock_result = MagicMock()
        mock_result.bindings = mock_bindings  # Mocking the `bindings` attribute
        db_manager._g["model"].query.return_value = mock_result

        # Call the query method with return_df=True to get a DataFrame
        df = db_manager.query(query_str, graph="model", return_df=True)

        # Verify the DataFrame contents
        expected_df = pd.DataFrame(mock_bindings)
        pd.testing.assert_frame_equal(df, expected_df)


def test_query_keyerror(db_manager):
    """Test the query method of the DBManager class with a KeyError."""
    graph = "unknown"
    with pytest.raises(KeyError, match="Unknown graph"):
        _ = db_manager.query("SELECT * WHERE { ?s ?p ?o }", graph=graph)


# def test_defrag_uri():
#     """Test the defrag_uri function of the DBManager class."""
#     uri = rdflib.URIRef("http://example.com#fragment")
#     assert DBManager.defrag_uri(uri) == "fragment"


def test_defrag_uri_with_fragment():
    """
    Test the defrag_uri function of the DBManager class with a URI with a
    fragment.
    """
    uri = rdflib.URIRef("http://example.org/resource#fragment")
    assert DBManager.defrag_uri(uri) == "fragment"


def test_defrag_uri_with_slash():
    """
    Test the defrag_uri function of the DBManager class with a URI without a
    fragment but with a path component.
    """
    uri = rdflib.URIRef("http://example.org/resource/path")
    assert DBManager.defrag_uri(uri) == "path"


def test_defrag_uri_string_with_fragment():
    """
    Test the defrag_uri function of the DBManager class with a string URI with
    a fragment identifier.
    """
    uri = "http://example.org/resource#fragment"
    assert DBManager.defrag_uri(uri) == "fragment"


def test_defrag_uri_string_with_slash():
    """
    Test the defrag_uri function of the DBManager class with a string URI
    without a fragment but with a path component.
    """
    uri = "http://example.org/resource/path"
    assert DBManager.defrag_uri(uri) == "path"


def test_defrag_uri_string_without_fragment_or_slash():
    """
    Test the defrag_uri function of the DBManager class with a string URI
    without a fragment or path component.
    """
    uri = "resource"
    assert DBManager.defrag_uri(uri) == "resource"


def test_defrag_uri_empty_string():
    """
    Test the defrag_uri function of the DBManager class with an empty string.
    """
    uri = ""
    assert DBManager.defrag_uri(uri) == ""


def test_defrag_uri_non_uri_string():
    """
    Test the defrag_uri function of the DBManager class with a non-URI string.
    """
    uri = "not_a_uri"
    assert DBManager.defrag_uri(uri) == "not_a_uri"

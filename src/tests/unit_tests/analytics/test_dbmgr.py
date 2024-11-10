"""Unit tests for the DBManager class in the analytics.dbmgr module."""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
import rdflib


from analytics.dbmgr import DBManager

# Suppress pylint warnings for fixtures
# pylint: disable=protected-access
# pylint: disable=redefined-outer-name
# pylint: disable=unused-argument

# Sample data for mocking
sample_mapper_data = pd.DataFrame(
    {
        "StreamID": ["stream1", "stream2", "stream3", "stream4"],
        "Filename": ["file1.pkl", "file2.pkl", "file3.pkl", "FILE NOT SAVED"],
        "Building": ["BuildingA", "BuildingB", "BuildingA", "BuildingA"],
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
                            data_zip_path="data.zip",
                            mapper_path="mapper.csv",
                            model_path="model.rdf",
                            schema_path="schema.rdf",
                            building="BuildingA",
                        )


def test_initialisation(db_manager):
    """Test DBManager initialisation with the db_manager fixture."""
    # Assertions to confirm initialization paths and data
    assert str(db_manager._data_zip_path) == "data.zip"
    assert str(db_manager._mapper_path) == "mapper.csv"
    assert str(db_manager._model_path) == "model.rdf"
    assert str(db_manager._schema_path) == "schema.rdf"
    assert "stream1" in db_manager.mapper["StreamID"].values
    assert "BuildingA" in db_manager.mapper["Building"].values


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


def test_defrag_uri():
    """Test the defrag_uri function of the DBManager class."""
    uri = rdflib.URIRef("http://example.com#fragment")
    assert DBManager.defrag_uri(uri) == "fragment"

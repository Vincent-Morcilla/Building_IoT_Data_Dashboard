"""Library for managing the database of streams"""

from collections.abc import Iterable
from pathlib import Path
import pickle
import zipfile

import brickschema
import pandas as pd
import rdflib


class DBManager:
    """Class for managing the database of streams

    Args:
        data_zip_path (str): Path to the zip file containing the stream data
        mapping_path (str): Path to the CSV file containing the mapping of stream IDs to filenames
        model_path (str): Path to the RDF file containing the Brick model
        schema_path (str): Path to the RDF file containing the Brick schema

    Raises:
        FileNotFoundError: If the data zip file is not found
        FileNotFoundError: If the mapping file is not found
        FileNotFoundError: If the model file is not found
        FileNotFoundError: If the schema file is not found

    Returns:
        DBManager: An instance of the DBManager class
    """

    def __init__(self, data_zip_path, mapping_path, model_path, schema_path=None):
        self._data_zip_path = Path(data_zip_path)
        self._mapping_path = Path(mapping_path)
        self._model_path = Path(model_path)
        self._schema_path = None if schema_path is None else Path(schema_path)

        if not self._data_zip_path.is_file():
            raise FileNotFoundError(f"Data zip file not found: {self._data_zip_path}")

        if not self._mapping_path.is_file():
            raise FileNotFoundError(f"Mapping file not found: {self._mapping_path}")

        if not self._model_path.is_file():
            raise FileNotFoundError(f"Model file not found: {self._model_path}")

        if self._schema_path is not None and not self._schema_path.is_file():
            raise FileNotFoundError(f"Schema file not found: {self._schema_path}")

        self._model = brickschema.Graph().load_file(self._model_path)

        if self._schema_path is not None:
            self._schema = brickschema.Graph().load_file(self._schema_path)
        else:
            self._schema = brickschema.Graph(load_brick_nightly=True)

        self._db = {}
        self._load_db()

    def __len__(self) -> int:
        """Get the number of streams in the database.

        Returns:
            int: The number of streams in the database.
        """
        return len(self._db)

    def __iter__(self) -> iter:
        """Iterate over the stream IDs in the database.

        Returns:
            iter: An iterator over the stream IDs.
        """
        return iter(self._db)

    def __contains__(self, key: str) -> bool:
        """Check if a stream ID is in the database.

        Args:
            key (str): The stream ID.

        Returns:
            bool: True if the stream ID is in the database, False otherwise.
        """
        return key in self._db

    def __getitem__(self, key: str) -> pd.DataFrame:
        """Get the stream data for a given stream ID."""
        return self._db[key]

    def __setitem__(self, key, value: pd.DataFrame):
        """Set the stream data for a given stream ID."""
        self._db[key] = value

    @property
    def model(self) -> brickschema.Graph:
        """The building model as knowledge graph.

        Returns:
            brickschema.Graph: The building model in RDF format.
        """
        return self._model

    @property
    def schema(self) -> brickschema.Graph:
        """The brick schema as knowledge graph.

        Returns:
            brickschema.Graph: The brick schema in RDF format.
        """
        return self._schema

    @property
    def data(self) -> dict[str, pd.DataFrame]:
        """The database of streams.

        Returns:
            dict[str, pd.DataFrame]: The database of streams.
        """
        return self._db

    def query(
        self, query_str: str, graph: str = "model", return_df: bool = False, **kwargs
    ) -> rdflib.query.Result | pd.DataFrame:
        """Query the knowledge graph.

        Args:
            query_str (str): The SPARQL query string.
            graph (str, optional): The graph to query. Defaults to "model".
            return_df (bool, optional): Whether to return the results as a DataFrame. Defaults to False.

        Returns:
            rdflib.query.Result | pd.DataFrame: The query results.
        """
        if graph == "model":
            graph = self._model
        elif graph == "schema":
            graph = self._schema
        else:
            raise ValueError(f"Unknown graph: {graph}")

        results = graph.query(query_str, **kwargs)

        if return_df:
            df = pd.DataFrame(results.bindings)
            df.columns = df.columns.map(str)
            df.drop_duplicates(inplace=True)
            return df
        else:
            return results

    def get_stream(self, stream_id: str) -> pd.DataFrame:
        """Get the stream data for a given stream ID.

        Args:
            stream_id (str): The stream ID.

        Returns:
            pd.DataFrame: The stream data.
        """
        try:
            return self._db[str(stream_id)]
        except KeyError as exc:
            raise KeyError(f"Stream ID {stream_id} not found in the database") from exc

    def get_streams(self, stream_ids: Iterable[str]) -> dict[str, pd.DataFrame]:
        """Get the stream data for a list of stream IDs.

        Args:
            stream_ids (Iterable[str]): The list of stream IDs.

        Returns:
            dict[str, pd.DataFrame]: A dictionary of stream data.
        """
        return {stream_id: self.get_stream(stream_id) for stream_id in stream_ids}

    def _load_db(self):
        mapping_df = pd.read_csv(self._mapping_path, index_col=0)

        # @tim: TODO: decide whether to keep/remove these filters
        # Mappings for building B only, and ignore streams not saved to file
        # mapping_df = mapping_df[mapping_df["Building"] == "B"]
        # mapping_df = mapping_df[
        #     mapping_df["Filename"].str.contains("FILE NOT SAVED") == False
        # ]

        with zipfile.ZipFile(self._data_zip_path, "r") as db_zip:
            for path in db_zip.namelist():
                if not path.endswith(".pkl"):
                    continue

                filename = Path(path).name
                record = mapping_df.loc[mapping_df["Filename"] == filename, "StreamID"]

                # ignore streams that don't have a mapping
                if record.empty:
                    continue

                stream_id = record.iloc[0]

                pkl_data = db_zip.read(path)
                data = pickle.loads(pkl_data)
                data_df = pd.DataFrame(data)
                data_df.rename(
                    columns={"t": "time", "v": "value", "y": "label"}, inplace=True
                )

                self._db[stream_id] = data_df

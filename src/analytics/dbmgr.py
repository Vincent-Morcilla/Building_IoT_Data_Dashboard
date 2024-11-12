"""
Module for managing the dataset of time series data, the building model, the
mapping file between the time series data and building model, and the brick 
schema.
"""

from collections.abc import Iterable
from pathlib import Path
import pickle
import zipfile

import brickschema
import pandas as pd
import rdflib
from tqdm import tqdm


class DBManagerFileNotFoundError(FileNotFoundError):
    """Raised when a required file is not found by DBManager."""


class DBManagerBadCsvFile(pd.errors.ParserError):
    """Raised when DBManager encounters an invalid CSV file."""


class DBManagerBadRdfFile(AttributeError):
    """Raised when DBManager encounters an invalid RDF file."""


class DBManagerBadZipFile(zipfile.BadZipFile):
    """Raised when DBManager encounters an invalid zip file."""


class DBManager:
    """Class for managing the dataset of time series data, the building model,
    the mapping file between the time series data and building model, and the
    brick schema.

    Args:
        data_zip_path (str): Path to the zip file containing the time series data.
        mapper_path (str): Path to the CSV file containing the mapping of
            stream IDs to files in the zip file.
        model_path (str): Path to the RDF file containing the Brick model.
        schema_path (str, optional): Path to the RDF file containing the Brick
            schema. Defaults to None, in which case the latest Brick schema will
            be used.
        building (str, optional): The building to filter the streams by. Defaults
            to None.

    Raises:
        DBManagerFileNotFoundError: If the data zip file is not found.
        DBManagerFileNotFoundError: If the mapping file is not found.
        DBManagerFileNotFoundError: If the model file is not found.
        DBManagerFileNotFoundError: If the schema file is not found.
    """

    def __init__(
        self, data_zip_path, mapper_path, model_path, schema_path=None, building=None
    ) -> None:
        """Initialise the DBManager.

        Args:
            data_zip_path (str): Path to the zip file containing the time series data.
            mapper_path (str): Path to the CSV file containing the mapping of
                stream IDs to files in the zip file.
            model_path (str): Path to the RDF file containing the Brick model.
            schema_path (str, optional): Path to the RDF file containing the Brick
                schema. Defaults to None, in which case the latest Brick schema will
                be used.
            building (str, optional): The building to filter the streams by. Defaults
                to None.

        Raises:
            DBManagerFileNotFoundError: If the data zip file is not found.
            DBManagerFileNotFoundError: If the mapping file is not found.
            DBManagerFileNotFoundError: If the model file is not found.
            DBManagerFileNotFoundError: If the schema file is not found.
        """
        self._data_zip_path = Path(data_zip_path)
        self._mapper_path = Path(mapper_path)
        self._model_path = Path(model_path)
        self._schema_path = None if schema_path is None else Path(schema_path)
        if not self._data_zip_path.is_file():
            raise DBManagerFileNotFoundError(
                f"Data zip file not found: {self._data_zip_path}"
            )

        if not self._mapper_path.is_file():
            raise DBManagerFileNotFoundError(
                f"Mapping file not found: {self._mapper_path}"
            )

        if not self._model_path.is_file():
            raise DBManagerFileNotFoundError(
                f"Model file not found: {self._model_path}"
            )

        if self._schema_path is not None and not self._schema_path.is_file():
            raise DBManagerFileNotFoundError(
                f"Schema file not found: {self._schema_path}"
            )

        try:
            self._mapper = pd.read_csv(self._mapper_path, index_col=0)
        except pd.errors.ParserError as exc:
            raise DBManagerBadCsvFile(
                f"Error reading CSV file: {self._mapper_path}"
            ) from exc

        # Filter the mapper to only include streams from the specified building
        if building is not None:
            self._mapper = self._mapper[self._mapper["Building"] == building]

        # Filter out streams that were not saved to file, i.e., are not in the
        # data zip file
        # pylint: disable=C0121
        self._mapper = self._mapper[
            self._mapper["Filename"].str.contains("FILE NOT SAVED") == False
        ]

        # the "model" graph is the building model, the "schema" graph is the
        # Brick schema, the "schema+model" graph is the combination of the
        # model and schema, and the "expanded_model" graph is the model with
        # the schema expanded
        try:
            self._g = {"model": brickschema.Graph().load_file(self._model_path)}
        except AttributeError as exc:
            raise DBManagerBadRdfFile(
                f"Error reading RDF file: {self._model_path}"
            ) from exc

        if self._schema_path is not None:
            try:
                self._g["schema"] = brickschema.Graph().load_file(self._schema_path)
                self._g["schema+model"] = brickschema.Graph().load_file(
                    self._schema_path
                )
                self._g["expanded_model"] = brickschema.Graph().load_file(
                    self._schema_path
                )
            except AttributeError as exc:
                raise DBManagerBadRdfFile(
                    f"Error reading RDF file: {self._schema_path}"
                ) from exc
        else:
            self._g["schema"] = brickschema.Graph(load_brick_nightly=True)
            self._g["schema+model"] = brickschema.Graph(load_brick_nightly=True)
            self._g["expanded_model"] = brickschema.Graph(load_brick_nightly=True)

        self._g["schema+model"].load_file(self._model_path)
        self._g["expanded_model"].load_file(self._model_path)
        self._g["expanded_model"].expand(profile="rdfs")

        # Load the stream data
        self._db = {}
        self._load_db()

    def __len__(self) -> int:
        """The number of streams in the database.

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

    def __setitem__(self, key, value: pd.DataFrame) -> None:
        """Set the stream data for a given stream ID."""
        self._db[key] = value

    @property
    def model(self) -> brickschema.Graph:
        """The building model as a knowledge graph.

        Returns:
            brickschema.Graph: The building model in RDF format.
        """
        return self._g["model"]

    @property
    def schema(self) -> brickschema.Graph:
        """The brick schema as a knowledge graph.

        Returns:
            brickschema.Graph: The brick schema in RDF format.
        """
        return self._g["schema"]

    @property
    def schema_and_model(self) -> brickschema.Graph:
        """The building model and brick schema as a knowledge graph.

        Returns:
            brickschema.Graph: The building model and brick schema in RDF format.
        """
        return self._g["schema+model"]

    @property
    def expanded_model(self) -> brickschema.Graph:
        """The building model and brick schema after inference a knowledge graph.

        Returns:
            brickschema.Graph: The expanded building model in RDF format.
        """
        return self._g["expanded_model"]

    @property
    def data(self) -> dict[str, pd.DataFrame]:
        """The database of streams.

        Returns:
            dict[str, pd.DataFrame]: The database of streams.
        """
        return self._db

    @property
    def mapper(self) -> pd.DataFrame:
        """The mapping of stream IDs to filenames.

        Returns:
            pd.DataFrame: The mapping of stream IDs to filenames.
        """
        return self._mapper

    def query(
        self,
        query_str: str,
        graph: str = "model",
        return_df: bool = False,
        defrag: bool = False,
        **kwargs,
    ) -> rdflib.query.Result | pd.DataFrame:
        """Query the building model or brick schema knowledge graph with a SPARQL
        query string.  Available graphs are:

        - "model": The building model.
        - "schema": The brick schema.
        - "schema+model": The combination of the model and schema.
        - "expanded_model": The model with the schema expanded, i.e. after
            inference.

        Args:
            query_str (str): The SPARQL query string.
            graph (str, optional): The graph to query. Defaults to "model".
            return_df (bool, optional): Whether to return the results as a
                DataFrame. Defaults to False.
            defrag (bool, optional): Whether to defragment the URIs in the
                DataFrame. Defaults to False.
            **kwargs: Additional keyword arguments to pass to the query.

        Returns:
            rdflib.query.Result | pd.DataFrame: The query results.

        Raises:
            KeyError: If the graph is not found.
        """
        try:
            graph = self._g[graph]
        except KeyError as exc:
            raise KeyError(f"Unknown graph: {graph}") from exc

        results = graph.query(query_str, **kwargs)

        if return_df:
            df = pd.DataFrame(results.bindings)
            df.columns = df.columns.map(str)
            df.drop_duplicates(inplace=True)

            if defrag:
                for col in df.columns:
                    df[col] = df[col].apply(DBManager.defrag_uri)
            return df

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
            raise KeyError(
                f"Stream ID '{stream_id}' not found in the database"
            ) from exc

    def set_stream(self, stream_id: str, data: pd.DataFrame) -> None:
        """Set the stream data for a given stream ID.

        Args:
            stream_id (str): The stream ID.
            data (pd.DataFrame): The stream data.
        """
        self._db[stream_id] = data

    def get_streams(self, stream_ids: Iterable[str]) -> dict[str, pd.DataFrame]:
        """Get the stream data for a list of stream IDs.

        Args:
            stream_ids (Iterable[str]): The list of stream IDs.

        Returns:
            dict[str, pd.DataFrame]: A dictionary of stream data.
        """
        return {stream_id: self.get_stream(stream_id) for stream_id in stream_ids}

    def set_streams(self, stream_data: dict[str, pd.DataFrame]) -> None:
        """Set the stream data for a list of stream IDs.

        Args:
            stream_data (dict[str, pd.DataFrame]): A dictionary of stream data.
        """
        for stream_id, data in stream_data.items():
            self.set_stream(stream_id, data)

    def get_all_streams(self) -> dict[str, pd.DataFrame]:
        """Get all streams in the database.

        Returns:
            dict[str, pd.DataFrame]: A dictionary of all stream data.
        """
        return self.data

    def get_stream_label(self, stream_id: str) -> str:
        """Get the Brick class of a given stream ID from the mapper file.

        Args:
            stream_id (str): The stream ID to get the Brick class for.

        Returns:
            str: The Brick class of the URI.
        """
        record = self._mapper.loc[
            self._mapper["StreamID"] == stream_id, "strBrickLabel"
        ]

        # raise an error if the stream ID is not found
        if record.empty:
            raise KeyError(f"Stream ID '{stream_id}' not found in the database")

        return record.iloc[0]

    def get_label(self, stream_id: str) -> str:
        """Get the label for a given stream ID.

        Args:
            stream_id (str): The stream ID.

        Returns:
            str: The label for the given stream ID.

        Raises:
            KeyError: If the stream ID is not found in the mapper.
        """
        return self.get_stream_label(stream_id)

    def _load_db(self) -> None:
        """Load the stream data from the zip file into the database."""

        try:
            with zipfile.ZipFile(self._data_zip_path, "r") as db_zip:
                # iterate through the files in the zip file
                for path in tqdm(db_zip.namelist(), desc="Reading stream data      "):
                    # ignore non-pickle files
                    if not path.endswith(".pkl"):
                        continue

                    # get the stream ID from the mapper
                    filename = Path(path).name
                    record = self._mapper.loc[
                        self._mapper["Filename"] == filename, "StreamID"
                    ]

                    # ignore streams that don't have a mapping
                    if record.empty:
                        continue

                    stream_id = record.iloc[0]

                    # load the stream data from the pickle file
                    pkl_data = db_zip.read(path)
                    data = pickle.loads(pkl_data)
                    data_df = pd.DataFrame(data)
                    data_df.rename(
                        columns={"t": "time", "v": "value", "y": "brick_class"},
                        inplace=True,
                    )

                    # set the stream data in the database
                    self._db[stream_id] = data_df
        except zipfile.BadZipFile as exc:
            raise DBManagerBadZipFile(
                f"Error reading zip file: {self._data_zip_path}"
            ) from exc

    @staticmethod
    def defrag_uri(uri: rdflib.term.URIRef | str) -> str:
        """Extract the fragment (last path component) from a URI.

        Args:
            uri (rdflib.term.URIRef): The URI to defragment.

        Returns:
            str: The defragmented URI.
        """
        if isinstance(uri, rdflib.term.URIRef):
            if "#" in uri:
                return uri.fragment
            if "/" in uri:
                return uri.split("/")[-1]
        return uri

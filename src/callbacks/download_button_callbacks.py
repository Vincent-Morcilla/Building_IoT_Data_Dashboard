import hashlib
import io
import zipfile
from typing import Any, Dict, Optional

import pandas as pd
from dash import Dash, dcc
from dash.dependencies import Input, Output
from dash.exceptions import PreventUpdate

from helpers.helpers import sanitise_filename


def hash_dataframe(df: pd.DataFrame) -> str:
    """
    Compute a unique MD5 hash for a DataFrame.

    Args:
        df: The DataFrame to hash.

    Returns:
        The MD5 hash of the DataFrame.
    """
    df_bytes = df.to_csv(index=False).encode("utf-8")
    return hashlib.md5(df_bytes).hexdigest()


def generate_filename(
    main_cat: str,
    sub_cat: str,
    component_id: str,
    title: str,
    comp_type: str,
    file_counters: Dict[str, int],
    trace_type: Optional[str] = None,
) -> str:
    """
    Generate a unique filename using the provided parameters.

    Args:
        main_cat: Main category.
        sub_cat: Subcategory.
        component_id: Component ID.
        title: Title of the plot/table.
        comp_type: Type of the component ('Plot', 'Table', 'UI', etc.).
        file_counters: Counter to ensure unique filenames.
        trace_type: Type of trace (e.g., 'Box', 'Line').

    Returns:
        A unique filename string.
    """
    base = f"{main_cat}_{sub_cat}_{component_id}_{title}_{comp_type}"
    if trace_type:
        base += f"_{trace_type}"
    base = sanitise_filename(base)
    count = file_counters.get(base, 1)
    filename = f"{base}_{count}.csv"
    file_counters[base] = count + 1
    return filename


def process_dataframe(
    source: Any,
    main_cat: str,
    sub_cat: str,
    component_id: str,
    title: str,
    comp_type: str,
    file_counters: Dict[str, int],
    processed_df_hashes: set,
    csv_files: list,
    trace_type: Optional[str] = None,
) -> None:
    """
    Process a DataFrame or a dictionary of DataFrames for inclusion in the ZIP file.

    Args:
        source: The DataFrame or dict of DataFrames to process.
        main_cat: Main category from plot_configs key.
        sub_cat: Subcategory from plot_configs key.
        component_id: ID of the component.
        title: Title of the plot/table.
        comp_type: Type of the component ('Plot', 'Table', 'UI', etc.).
        file_counters: Counter for unique filenames.
        processed_df_hashes: Set to track processed DataFrame hashes.
        csv_files: List to collect (filename, csv_bytes) tuples.
        trace_type: Type of trace.

    """
    if isinstance(source, pd.DataFrame):
        df_hash = hash_dataframe(source)
        if df_hash in processed_df_hashes:
            return
        processed_df_hashes.add(df_hash)

        filename = generate_filename(
            main_cat, sub_cat, component_id, title, comp_type, file_counters, trace_type
        )
        csv_bytes = source.to_csv(index=False).encode("utf-8")
        csv_files.append((filename, csv_bytes))

    elif isinstance(source, dict):
        for key, df in source.items():
            if isinstance(df, pd.DataFrame):
                df_hash = hash_dataframe(df)
                if df_hash in processed_df_hashes:
                    continue
                processed_df_hashes.add(df_hash)

                title_extended = f"{title}_{key}"
                filename = generate_filename(
                    main_cat,
                    sub_cat,
                    component_id,
                    title_extended,
                    comp_type,
                    file_counters,
                )
                csv_bytes = df.to_csv(index=False).encode("utf-8")
                csv_files.append((filename, csv_bytes))


def locate_component(
    plot_configs: Dict[Any, Any], component_id: str
) -> Optional[Dict[str, Any]]:
    """
    Locate a component by its ID within plot_configs.

    Args:
        plot_configs: The plot configurations dictionary.
        component_id: The ID of the component to locate.

    Returns:
        The component dictionary if found, else None.
    """
    for config in plot_configs.values():
        for component in config.get("components", []):
            if component.get("id") == component_id:
                return component
    return None


def extract_dataframe_from_component(
    component: Dict[str, Any]
) -> Optional[pd.DataFrame]:
    """
    Extract the DataFrame from a given component.

    Args:
        component: The component dictionary.

    Returns:
        The extracted DataFrame, if any.
    """
    comp_type = component.get("type")
    df = None

    if comp_type == "plot":
        df = component.get("kwargs", {}).get("data_frame")
        if df is None and component.get("library") == "go":
            df = component.get("data_frame")

    elif comp_type == "table":
        df = component.get("dataframe")
        if df is None:
            data = component.get("kwargs", {}).get("data")
            if isinstance(data, list):
                df = pd.DataFrame(data)

    elif comp_type == "UI":
        if component.get("element") == "DataTable":
            data = component.get("kwargs", {}).get("data")
            if isinstance(data, list):
                df = pd.DataFrame(data)

    return df if isinstance(df, pd.DataFrame) else None


class DownloadManager:
    """
    Manages the preparation and sending of downloadable ZIP files containing CSVs of DataFrames.
    """

    def __init__(self):
        """Initialize the DownloadManager with empty attributes."""
        self.csv_files: list = []
        self.file_counters: Dict[str, int] = {}
        self.processed_df_hashes: set = set()

    def add_csv_file(self, filename: str, data: bytes) -> None:
        """
        Add a CSV file to the list of files to be zipped.

        Args:
            filename: The name of the CSV file.
            data: The CSV data in bytes.
        """
        self.csv_files.append((filename, data))

    def prepare_zip(self) -> io.BytesIO:
        """
        Create a ZIP file in memory containing all added CSV files.

        Returns:
            The in-memory ZIP file.
        """
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:
            for filename, data in self.csv_files:
                zip_file.writestr(filename, data)
        zip_buffer.seek(0)
        return zip_buffer

    def send_zip(self, zip_buffer: io.BytesIO) -> Dict[str, Any]:
        """
        Create a downloadable response for the ZIP file.

        Args:
            zip_buffer: The in-memory ZIP file.

        Returns:
            The Dash downloadable data.
        """

        def write_zip(file_like):
            file_like.write(zip_buffer.read())

        return dcc.send_bytes(write_zip, "All_Dataframes.zip")


def register_download_callbacks(app: Dash, plot_configs: Dict[Any, Any]) -> None:
    """
    Register callbacks to handle global download functionality.

    Args:
        app: The Dash app instance.
        plot_configs: The plot configurations dictionary.
    """

    @app.callback(
        Output("global-download-data", "data"),
        Input("global-download-button", "n_clicks"),
        prevent_initial_call=True,
    )
    def download_all_data(n_clicks: int) -> Dict[str, Any]:
        """
        Handle downloading all DataFrames in plot_configs as CSVs within a ZIP file.

        Args:
            n_clicks: Number of times the download button has been clicked.

        Returns:
            The data to be downloaded.
        """
        if not n_clicks:
            raise PreventUpdate

        download_manager = DownloadManager()

        # Traverse plot_configs to find all DataFrames
        for config_key, config in plot_configs.items():
            if not isinstance(config_key, tuple) or len(config_key) != 2:
                continue

            main_category, subcategory = config_key
            sanitized_main = sanitise_filename(main_category.lower())
            sanitized_sub = sanitise_filename(subcategory.lower())

            # Process Components
            for component in config.get("components", []):
                comp_type = component.get("type")
                if comp_type not in ["plot", "table", "UI"]:
                    continue  # Ignore other component types like 'separator'

                component_id = component.get("id")
                if not component_id:
                    continue

                title = ""
                trace_type = None

                if comp_type == "plot":
                    library = component.get("library")
                    layout_kwargs = component.get("layout_kwargs", {})
                    plot_title = layout_kwargs.get("title", {}).get("text", "Plot")

                    plot_function = component.get("function", "plot")
                    plot_type = plot_function.capitalize()
                    trace_type = plot_type

                    data_frame = component.get("kwargs", {}).get("data_frame")

                    if data_frame is None and library == "go":
                        data_frame = component.get("data_frame")

                    if data_frame is not None and isinstance(data_frame, pd.DataFrame):
                        title = plot_title.replace(" ", "_").lower()
                        process_dataframe(
                            source=data_frame,
                            main_cat=sanitized_main,
                            sub_cat=sanitized_sub,
                            component_id=component_id,
                            title=title,
                            comp_type="Plot",
                            file_counters=download_manager.file_counters,
                            processed_df_hashes=download_manager.processed_df_hashes,
                            csv_files=download_manager.csv_files,
                            trace_type=trace_type,
                        )

                elif comp_type == "table":
                    columns = component.get("kwargs", {}).get("columns", [])
                    if columns:
                        table_title = columns[0].get("name", "Table")
                    else:
                        table_title = "Table"
                    title = table_title.replace(" ", "_").lower()

                    data_frame = component.get("dataframe")
                    if data_frame is None:
                        data = component.get("kwargs", {}).get("data")
                        if isinstance(data, list):
                            data_frame = pd.DataFrame(data)

                    if data_frame is not None and isinstance(data_frame, pd.DataFrame):
                        process_dataframe(
                            source=data_frame,
                            main_cat=sanitized_main,
                            sub_cat=sanitized_sub,
                            component_id=component_id,
                            title=title,
                            comp_type="Table",
                            file_counters=download_manager.file_counters,
                            processed_df_hashes=download_manager.processed_df_hashes,
                            csv_files=download_manager.csv_files,
                        )

                elif comp_type == "UI":
                    element = component.get("element")
                    if element != "DataTable":
                        continue  # Only handle DataTable for DataFrames

                    data = component.get("kwargs", {}).get("data")
                    if not isinstance(data, list) or not data:
                        continue
                    data_frame = pd.DataFrame(data)
                    title = element.replace(" ", "_").lower()
                    process_dataframe(
                        source=data_frame,
                        main_cat=sanitized_main,
                        sub_cat=sanitized_sub,
                        component_id=component_id,
                        title=title,
                        comp_type="UI",
                        file_counters=download_manager.file_counters,
                        processed_df_hashes=download_manager.processed_df_hashes,
                        csv_files=download_manager.csv_files,
                    )

            # Process Interactions
            for interaction in config.get("interactions", []):
                data_source = interaction.get("data_source", {})
                if not data_source:
                    continue

                data_dict = data_source.get("data_dict")
                if data_dict:
                    # Loop through each item in data_dict
                    for index_value, components in data_dict.items():
                        index_value_sanitized = sanitise_filename(str(index_value))
                        for component in components:
                            comp_type = component.get("type")
                            component_id = component.get("id")
                            if not component_id:
                                continue

                            title = ""
                            df = None
                            if comp_type == "plot":
                                library = component.get("library")
                                if library == "px":
                                    df = component.get("kwargs", {}).get("data_frame")
                                elif library == "go":
                                    df = component.get("data_frame")
                                title = component.get("kwargs", {}).get(
                                    "title", component_id
                                )
                            elif comp_type == "table":
                                df = component.get("dataframe")
                                title = component.get("title", component_id)
                            elif comp_type == "UI":
                                if component.get("element") == "DataTable":
                                    data = component.get("kwargs", {}).get("data")
                                    if isinstance(data, list):
                                        df = pd.DataFrame(data)
                                    title = component.get("label", component_id)

                            if df is not None and isinstance(df, pd.DataFrame):
                                # Generate filename
                                filename = generate_filename(
                                    sanitized_main,
                                    sanitized_sub,
                                    f"{component_id}_{index_value_sanitized}",
                                    title or component_id,
                                    comp_type.capitalize(),
                                    download_manager.file_counters,
                                )
                                csv_bytes = df.to_csv(index=False).encode("utf-8")
                                download_manager.add_csv_file(filename, csv_bytes)

                elif isinstance(data_source, dict):
                    process_dataframe(
                        source=data_source,
                        main_cat=sanitized_main,
                        sub_cat=sanitized_sub,
                        component_id="Interaction",
                        title="interaction",
                        comp_type="Interaction",
                        file_counters=download_manager.file_counters,
                        processed_df_hashes=download_manager.processed_df_hashes,
                        csv_files=download_manager.csv_files,
                    )
                elif isinstance(data_source, str):
                    component = locate_component(plot_configs, data_source)
                    if component:
                        df = extract_dataframe_from_component(component)
                        if df is not None and isinstance(df, pd.DataFrame):
                            title = (
                                component.get("id")
                                .replace("-", "_")
                                .replace(" ", "_")
                                .lower()
                            )
                            process_dataframe(
                                source=df,
                                main_cat=sanitized_main,
                                sub_cat=sanitized_sub,
                                component_id=component.get("id")
                                .replace("-", "_")
                                .lower(),
                                title=title,
                                comp_type=component.get("type"),
                                file_counters=download_manager.file_counters,
                                processed_df_hashes=download_manager.processed_df_hashes,
                                csv_files=download_manager.csv_files,
                            )
                else:
                    continue

        if not download_manager.csv_files:
            raise PreventUpdate

        zip_buffer = download_manager.prepare_zip()
        return download_manager.send_zip(zip_buffer)

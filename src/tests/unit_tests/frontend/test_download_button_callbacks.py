import pytest
import pandas as pd
import hashlib
from zipfile import ZipFile
from callbacks.download_button_callbacks import (
    hash_dataframe,
    generate_filename,
    process_dataframe,
    locate_component,
    extract_dataframe_from_component,
    DownloadManager,
)


def test_hash_dataframe():
    """Test hash generation for a DataFrame."""
    df = pd.DataFrame({"col1": [1, 2], "col2": [3, 4]})
    expected_hash = hashlib.md5(df.to_csv(index=False).encode("utf-8")).hexdigest()
    assert hash_dataframe(df) == expected_hash


def test_generate_filename():
    """Test filename generation with and without trace type."""
    file_counters = {}
    filename = generate_filename(
        "main", "sub", "comp1", "title", "Plot", file_counters, "Line"
    )
    assert filename == "main_sub_comp1_title_Plot_Line_1.csv"
    filename_no_trace = generate_filename(
        "main", "sub", "comp1", "title", "Table", file_counters
    )
    assert filename_no_trace == "main_sub_comp1_title_Table_1.csv"
    assert file_counters["main_sub_comp1_title_Plot_Line"] == 2


def test_process_dataframe_single_df():
    """Test processing of a single DataFrame."""
    df = pd.DataFrame({"col1": [1, 2], "col2": [3, 4]})
    file_counters = {}
    processed_df_hashes = set()
    csv_files = []
    process_dataframe(
        df,
        "main",
        "sub",
        "comp1",
        "title",
        "Plot",
        file_counters,
        processed_df_hashes,
        csv_files,
    )
    assert len(csv_files) == 1
    filename, data = csv_files[0]
    assert filename == "main_sub_comp1_title_Plot_1.csv"


def test_process_dataframe_dict_of_dfs():
    """Test processing of a dictionary of DataFrames."""
    dfs = {"df1": pd.DataFrame({"col1": [1]}), "df2": pd.DataFrame({"col1": [2]})}
    file_counters = {}
    processed_df_hashes = set()
    csv_files = []
    process_dataframe(
        dfs,
        "main",
        "sub",
        "comp2",
        "table_title",
        "Table",
        file_counters,
        processed_df_hashes,
        csv_files,
    )
    assert len(csv_files) == 2
    assert csv_files[0][0] == "main_sub_comp2_table_title_df1_Table_1.csv"
    assert csv_files[1][0] == "main_sub_comp2_table_title_df2_Table_1.csv"


def test_locate_component():
    """Test locating a component within plot configs."""
    plot_configs = {
        ("main", "sub"): {
            "components": [
                {"id": "comp1", "type": "plot"},
                {"id": "comp2", "type": "table"},
            ]
        }
    }
    component = locate_component(plot_configs, "comp1")
    assert component == {"id": "comp1", "type": "plot"}
    assert locate_component(plot_configs, "nonexistent") is None


def test_extract_dataframe_from_component():
    """Test extracting a DataFrame from various component types."""
    plot_component = {
        "type": "plot",
        "kwargs": {"data_frame": pd.DataFrame({"col": [1]})},
    }
    table_component = {"type": "table", "kwargs": {"data": [{"col": 2}]}}
    ui_component = {
        "type": "UI",
        "element": "DataTable",
        "kwargs": {"data": [{"col": 3}]},
    }

    assert extract_dataframe_from_component(plot_component).equals(
        pd.DataFrame({"col": [1]})
    )
    assert extract_dataframe_from_component(table_component).equals(
        pd.DataFrame({"col": [2]})
    )
    assert extract_dataframe_from_component(ui_component).equals(
        pd.DataFrame({"col": [3]})
    )


def test_download_manager_add_csv_file():
    """Test adding a CSV file to the DownloadManager."""
    manager = DownloadManager()
    data = b"col1,col2\n1,2\n3,4\n"
    manager.add_csv_file("test.csv", data)
    assert manager.csv_files == [("test.csv", data)]


def test_download_manager_prepare_zip():
    """Test preparing a ZIP file in DownloadManager."""
    manager = DownloadManager()
    data = b"col1,col2\n1,2\n3,4\n"
    manager.add_csv_file("test.csv", data)
    zip_buffer = manager.prepare_zip()

    with ZipFile(zip_buffer, "r") as zip_file:
        assert "test.csv" in zip_file.namelist()
        assert zip_file.read("test.csv") == data


def test_download_manager_send_zip():
    """Test preparing ZIP data for download."""
    manager = DownloadManager()
    data = b"col1,col2\n1,2\n3,4\n"
    manager.add_csv_file("test.csv", data)
    zip_buffer = manager.prepare_zip()
    download_data = manager.send_zip(zip_buffer)
    assert download_data is not None

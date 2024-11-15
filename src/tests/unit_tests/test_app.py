"""Unit tests for the app module."""

from unittest.mock import patch, MagicMock

from dash import Dash, html
import pytest


from app import create_app
from app import main
from app import parse_args
from models.types import PlotConfig
from analytics.dbmgr import DBManagerFileNotFoundError


@patch("sys.argv", ["program_name"])
def test_parse_args_default():
    """Test with no arguments, should raise an error for missing files."""
    with pytest.raises(SystemExit):  # Expecting a SystemExit due to missing files
        parse_args(None)


@patch("sys.argv", ["program_name", "--debug", "--test-mode"])
def test_parse_args_debug():
    """Test with debug mode enabled."""
    args = parse_args(None)
    assert args.debug is True
    assert args.host == "127.0.0.1"
    assert args.port == 8050


@patch(
    "sys.argv", ["program_name", "--host", "0.0.0.0", "--port", "8080", "--test-mode"]
)
def test_parse_args_host_port():
    """Test with custom host and port."""
    args = parse_args(None)
    assert args.host == "0.0.0.0"
    assert args.port == 8080


@patch(
    "sys.argv",
    ["program_name", "data.zip", "mapper.csv", "model.ttl", "schema.ttl"],
)
def test_parse_args_custom_schema():
    """Test with custom schema file."""
    args = parse_args(None)
    assert args.schema == "schema.ttl"
    assert args.data == "data.zip"
    assert args.mapper == "mapper.csv"
    assert args.model == "model.ttl"


@patch("sys.argv", ["program_name", "--test-mode"])
def test_parse_args_test_mode():
    """Test with test mode enabled."""
    args = parse_args(None)
    assert args.test_mode is True
    assert args.data is None
    assert args.mapper is None
    assert args.model is None
    assert args.schema is None


@patch(
    "sys.argv", ["program_name", "--test-mode", "data.zip", "mapper.csv", "model.ttl"]
)
def test_parse_args_test_mode_with_files():
    """Test with test mode enabled and additional files."""
    with pytest.raises(
        SystemExit
    ):  # Expecting error because files shouldn't be passed with --test-mode
        parse_args(None)


# Test the create_app function
@pytest.fixture
def mock_create_app_dependencies():
    """Mock the dependencies for the create_app function."""
    # Mock return values for external dependencies
    with patch(
        "app.create_category_structure"
    ) as mock_create_category_structure, patch(
        "app.create_layout"
    ) as mock_create_layout, patch(
        "app.register_download_callbacks"
    ) as mock_register_download_callbacks, patch(
        "app.register_general_callbacks"
    ) as mock_register_general_callbacks, patch(
        "app.register_plot_callbacks"
    ) as mock_register_plot_callbacks:

        mock_create_category_structure.return_value = (
            {"category1": "Category 1", "category2": "Category 2"},
            {"key1": "Category 1", "key2": "Category 2"},
            {"subkey1": "Subcategory 1", "subkey2": "Subcategory 2"},
        )

        # Return a valid Dash component for the layout mock
        mock_create_layout.return_value = html.Div("Mock Layout")

        # Sample plot config to pass into create_app
        plot_configs = MagicMock(PlotConfig)

        # Yield the mock objects for use in the test
        yield mock_create_category_structure, mock_create_layout, mock_register_download_callbacks, mock_register_general_callbacks, mock_register_plot_callbacks, plot_configs


def test_create_app(mock_create_app_dependencies):
    """Test the create_app function."""
    # Unpack the mock dependencies
    (
        mock_create_category_structure,
        mock_create_layout,
        mock_register_download_callbacks,
        mock_register_general_callbacks,
        mock_register_plot_callbacks,
        plot_configs,
    ) = mock_create_app_dependencies

    # Create the app using the create_app function
    app = create_app(plot_configs)

    # Assert that the app is an instance of Dash
    assert isinstance(app, Dash)

    # Assert that layout was set
    mock_create_layout.assert_called_once()

    # Assert that category structure was created
    mock_create_category_structure.assert_called_once_with(plot_configs.keys())

    # Assert that callbacks were registered
    mock_register_download_callbacks.assert_called_once_with(app, plot_configs)
    mock_register_general_callbacks.assert_called_once_with(
        app, plot_configs, mock_create_category_structure.return_value
    )
    mock_register_plot_callbacks.assert_called_once_with(app, plot_configs)

    # Assert that the app has the correct title
    assert app.title == "Green Insight"


# Test the main function for command-line execution
@pytest.fixture
def mock_main_dependencies_test_mode():
    """Mock the dependencies for the main function in test mode."""
    # Mock the necessary functions that are called in the main function
    with patch("app.parse_args") as mock_parse_args, patch(
        "app.create_app"
    ) as mock_create_app, patch("sys.exit") as mock_sys_exit:

        # Mock return values for parse_args
        mock_parse_args.return_value = MagicMock(
            test_mode=True,
        )

        # Mock create_app to return a mock app instance
        mock_create_app.return_value = MagicMock()

        # Yield the mocks for use in the test
        yield mock_parse_args, mock_create_app, mock_sys_exit


def test_main_test_mode(mock_main_dependencies_test_mode):
    """Test the main function in test mode."""
    # Unpack the mock dependencies
    (
        mock_parse_args,
        mock_create_app,
        mock_sys_exit,
    ) = mock_main_dependencies_test_mode

    # Call the main function
    main()

    # Assert parse_args was called to parse command-line arguments
    mock_parse_args.assert_called_once()

    # Assert create_app was called with the correct plot_configs (mock return value from AnalyticsManager)
    mock_create_app.assert_called_once()

    # Assert sys.exit was not called (indicating no errors in the process)
    mock_sys_exit.assert_not_called()


# Test the main function for command-line execution
@pytest.fixture
def mock_main_dependencies():
    """Mock the dependencies for the main function."""
    # Mock the necessary functions that are called in the main function
    with patch("app.parse_args") as mock_parse_args, patch(
        "app.create_app"
    ) as mock_create_app, patch("app.DBManager") as mock_DBManager, patch(
        "app.AnalyticsManager"
    ) as mock_AnalyticsManager, patch(
        "sys.exit"
    ) as mock_sys_exit:

        # Mock return values for parse_args
        mock_parse_args.return_value = MagicMock(
            test_mode=False,
            data="data.zip",
            mapper="mapper.csv",
            model="model.ttl",
            schema=None,
            debug=False,
            host="127.0.0.1",
            port=8050,
            building=None,
        )

        # Mock the DBManager and AnalyticsManager objects so they don't perform any real actions
        mock_db_instance = MagicMock()
        mock_DBManager.return_value = mock_db_instance

        mock_analytics_instance = MagicMock()
        mock_AnalyticsManager.return_value = mock_analytics_instance

        # Mock create_app to return a mock app instance
        mock_create_app.return_value = MagicMock()

        # Yield the mocks for use in the test
        yield mock_parse_args, mock_create_app, mock_DBManager, mock_AnalyticsManager, mock_sys_exit


def test_main(mock_main_dependencies):
    """Test the main function."""
    # Unpack the mock dependencies
    (
        mock_parse_args,
        mock_create_app,
        mock_DBManager,
        mock_AnalyticsManager,
        mock_sys_exit,
    ) = mock_main_dependencies

    # Call the main function
    main()

    # Assert parse_args was called to parse command-line arguments
    mock_parse_args.assert_called_once()

    # Assert DBManager was called with the expected arguments
    mock_DBManager.assert_called_once_with(
        "data.zip", "mapper.csv", "model.ttl", None, None
    )

    # Assert AnalyticsManager was created and its methods weren't actually run (mocked)
    mock_AnalyticsManager.assert_called_once()

    # Assert create_app was called with the correct plot_configs (mock return value from AnalyticsManager)
    mock_create_app.assert_called_once()

    # Assert sys.exit was not called (indicating no errors in the process)
    mock_sys_exit.assert_not_called()


def test_main_raises_dbmanagerfilenotfounderror():
    """Test the main function when DBManager raises a DBManagerFileNotFoundError."""
    # Mock the arguments passed to the function
    mock_args = [
        "fake_data_path",
        "fake_mapper_path",
        "fake_model_path",
        "fake_schema_path",
    ]

    # Patch the `parse_args` function to return mocked args
    with patch("app.parse_args") as mock_parse_args:
        mock_parse_args.return_value = MagicMock(
            data="fake_data_path",
            mapper="fake_mapper_path",
            model="fake_model_path",
            schema="fake_schema_path",
            building="fake_building_path",
            test_mode=False,
            debug=False,
            host="127.0.0.1",
            port=8050,
        )

        # Patch the `DBManager` to raise DBManagerFileNotFoundError
        with patch(
            "analytics.dbmgr.DBManager",
            side_effect=DBManagerFileNotFoundError("File not found"),
        ):
            with pytest.raises(SystemExit) as exc_info:
                main(mock_args)
                assert exc_info.type == SystemExit

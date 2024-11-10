"""Unit tests for the analyticsmgr module in the analytics package."""

from unittest.mock import MagicMock

import pytest
from analytics.analyticsmgr import AnalyticsManager


# Fixture to provide a mock DBManager
@pytest.fixture
def mock_db():
    """Fixture to provide a mock DBManager instance."""
    return MagicMock()


# pylint: disable=redefined-outer-name
def test_analytics_manager_initialisation(mock_db, mocker):
    """
    Unit test for the AnalyticsManager class initialisation.
    """
    # Mock os.listdir to control which files are in the modules directory
    mocker.patch(
        "os.listdir",
        return_value=["module1.py", "module2.py", "__init__.py", "non_py_file.txt"],
    )
    # Mock importlib to simulate dynamic imports
    mock_import_module = mocker.patch("importlib.import_module")
    mock_import_module.side_effect = (
        lambda name: MagicMock()
    )  # Simulate imported modules

    manager = AnalyticsManager(mock_db)

    # Check if only .py files excluding __init__.py are imported
    # pylint: disable=protected-access
    assert len(manager._modules) == 2
    mock_import_module.assert_any_call("module1")
    mock_import_module.assert_any_call("module2")


# pylint: disable=redefined-outer-name
def test_run_analytics_collects_plot_configs(mock_db, mocker):
    """
    Unit test for the run_analytics method in the AnalyticsManager class.
    """
    # Mock os.listdir and importlib to simulate two modules with mock run methods
    mocker.patch("os.listdir", return_value=["module1.py", "module2.py"])
    mock_module1 = MagicMock()
    mock_module2 = MagicMock()
    mock_module1.run.return_value = {"plot1": {"config1": "value1"}}
    mock_module2.run.return_value = {"plot2": {"config2": "value2"}}

    # Define a side effect function to return the correct mock module based on the module name
    def import_module_side_effect(name):
        if name == "module1":
            return mock_module1
        elif name == "module2":
            return mock_module2
        else:
            raise ImportError(f"Module {name} not found")

    mocker.patch("importlib.import_module", side_effect=import_module_side_effect)
    # mock_tqdm = mocker.patch(
    #     "analytics.analyticsmgr.tqdm", side_effect=lambda x, desc: x
    # )  # Mock tqdm to bypass progress bar

    manager = AnalyticsManager(mock_db)
    plot_configs = manager.run_analytics()

    # Check if run_analytics returns the combined plot configurations from both modules
    expected_configs = {"plot1": {"config1": "value1"}, "plot2": {"config2": "value2"}}
    assert plot_configs == expected_configs
    mock_module1.run.assert_called_once_with(mock_db)
    mock_module2.run.assert_called_once_with(mock_db)


# pylint: disable=redefined-outer-name
def test_run_analytics_handles_module_import_error(mock_db, mocker, capsys):
    """
    Unit test for the run_analytics method in the AnalyticsManager class
    when a module fails to import.
    """
    # Mock os.listdir to include a module that will fail to import
    mocker.patch("os.listdir", return_value=["module1.py"])

    # Mock importlib to raise ImportError when trying to import the module
    # pylint: disable=unused-variable
    mock_import_module = mocker.patch(
        "importlib.import_module", side_effect=ImportError("Import failed")
    )

    # pylint: disable=unused-variable
    manager = AnalyticsManager(mock_db)

    # Verify that the ImportError is printed to stderr
    captured = capsys.readouterr()
    assert "Failed to import module1" in captured.err


# pylint: disable=redefined-outer-name
def test_run_analytics_raises_exception_on_module_runtime_error(mock_db, mocker):
    """
    Unit test for the run_analytics method in the AnalyticsManager class
    when a module raises a runtime error.
    """
    # Mock os.listdir to simulate one valid module
    mocker.patch("os.listdir", return_value=["module1.py"])
    mock_module = MagicMock()
    mock_module.run.side_effect = RuntimeError("Unexpected runtime error")

    mocker.patch("importlib.import_module", return_value=mock_module)
    mocker.patch("analyticsmgr.tqdm", side_effect=lambda x, desc: x)  # Mock tqdm

    manager = AnalyticsManager(mock_db)

    # Expect that run_analytics raises the RuntimeError from module execution
    with pytest.raises(RuntimeError, match="Unexpected runtime error"):
        manager.run_analytics()

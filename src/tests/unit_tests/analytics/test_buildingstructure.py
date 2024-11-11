"""
Unit tests for the buildingstructure module in the analytics package.
This module tests the `_get_building_hierarchy`, `_get_building_area`, and `run`
functions to ensure they correctly retrieve and process building structure data.
"""

import pandas as pd
from analytics.dbmgr import DBManager
import analytics.modules.buildingstructure as bldg


def test_get_building_hierarchy_success(mocker):
    """
    Unit test for the `_get_building_hierarchy` function in the buildingstructure module.
    This test verifies that the function correctly retrieves building hierarchy data
    from a mock DBManager instance with sample data.
    """
    # Mock DBManager instance and its query results
    mock_db = mocker.Mock(spec=DBManager)

    # Sample data simulating a database response with building hierarchy information
    expected_df = pd.DataFrame(
        {
            "parent": ["Building", "Building", "Floor"],
            "parentLabel": ["Main Building", "Main Building", "Floor 1"],
            "child": ["Floor", "HVAC_Zone", "Conference_Room"],
            "childLabel": ["Floor 1", "HVAC Zone", "Conference Room"],
            "entityType": ["Location", "Location", "Location"],
        }
    )

    # Configure the mock DBManager to return the sample data when queried
    mock_db.query.return_value = expected_df

    # Call the function to retrieve the data
    result_df = bldg._get_building_hierarchy(mock_db)

    # Assertions
    # Check that the function returns a DataFrame
    assert isinstance(result_df, pd.DataFrame), "Expected a DataFrame result"

    # Check that the returned DataFrame matches the expected structure and values
    pd.testing.assert_frame_equal(result_df, expected_df)

    # Ensure the query was executed exactly once
    mock_db.query.assert_called_once()


def test_get_building_area_success(mocker):
    """
    Unit test for the `_get_building_area` function in the buildingstructure module.
    This test verifies that the function correctly retrieves building area hierarchy data
    from a mock DBManager instance with sample data.
    """
    # Mock DBManager instance and its query results
    mock_db = mocker.Mock(spec=DBManager)

    # Sample data simulating a database response with building area hierarchy information
    expected_df = pd.DataFrame(
        {
            "parent": ["Building", "Floor"],
            "parentLabel": ["Main Building", "Floor 1"],
            "child": ["Floor", "Conference_Room"],
            "childLabel": ["Floor 1", "Conference Room"],
            "entityType": ["Location", "Location"],
        }
    )

    # Configure the mock DBManager to return the sample data when queried
    mock_db.query.return_value = expected_df

    # Call the function to retrieve the data
    result_df = bldg._get_building_area(mock_db)

    # Assertions
    # Check that the function returns a DataFrame
    assert isinstance(result_df, pd.DataFrame), "Expected a DataFrame result"

    # Check that the returned DataFrame matches the expected structure and values
    pd.testing.assert_frame_equal(result_df, expected_df)

    # Ensure the query was executed exactly once
    mock_db.query.assert_called_once()


def test_run_empty_hierarchy(mocker):
    """
    Unit test for the `run` function in the buildingstructure module with an empty
    database response for building hierarchy data. Ensures that the function returns an
    empty configuration dictionary when no hierarchy data is available.
    """
    # Mock DBManager instance
    mock_db = mocker.Mock(spec=DBManager)

    # Patch `_get_building_hierarchy` and `_get_building_area` to return empty DataFrames
    mocker.patch(
        "analytics.modules.buildingstructure._get_building_hierarchy",
        return_value=pd.DataFrame(),
    )
    mocker.patch(
        "analytics.modules.buildingstructure._get_building_area",
        return_value=pd.DataFrame(),
    )

    # Run the function and capture the result
    result = bldg.run(mock_db)

    # Assertion: Check that the function returns an empty dictionary
    assert (
        not result
    ), "Expected an empty dictionary when no hierarchy data is available"


def test_run_with_hierarchy_and_area(mocker):
    """
    Integration test for the `run` function in the buildingstructure module with
    sample hierarchy and area data. This test verifies that the function correctly
    processes data and returns a configuration dictionary for visualization.
    """
    # Mock DBManager instance
    mock_db = mocker.Mock(spec=DBManager)

    # Sample data simulating building hierarchy
    hierarchy_data = pd.DataFrame(
        {
            "parent": ["Building", "Building", "Floor"],
            "parentLabel": ["Main Building", "Main Building", "Floor 1"],
            "child": ["Floor", "HVAC_Zone", "Conference_Room"],
            "childLabel": ["Floor 1", "HVAC ZOne", "Conference Room"],
            "entityType": ["Location", "Location", "Location"],
        }
    )

    # Sample data simulating building area
    area_data = pd.DataFrame(
        {
            "parent": ["Building", "Floor"],
            "parentLabel": ["Main Building", "Floor 1"],
            "child": ["Floor", "Conference_Room"],
            "childLabel": ["Floor 1", "Conference Room"],
            "entityType": ["Location", "Location"],
        }
    )

    # Configure the mock DBManager to return hierarchy and area data
    mocker.patch(
        "analytics.modules.buildingstructure._get_building_hierarchy",
        return_value=hierarchy_data,
    )
    mocker.patch(
        "analytics.modules.buildingstructure._get_building_area",
        return_value=area_data,
    )

    # Run the `run` function with the mocked DBManager
    config = bldg.run(mock_db)

    # Assertions
    # Verify that the configuration dictionary contains keys for both BuildingHierarchy and BuildingLocations
    assert (
        "BuildingStructure",
        "BuildingHierarchy",
    ) in config, "Expected BuildingHierarchy in config"
    assert (
        "BuildingStructure",
        "BuildingLocations",
    ) in config, "Expected BuildingLocations in config"

    # Check that each config entry has the expected components structure
    for key in config:
        assert (
            "components" in config[key]
        ), f"Missing components in config entry for {key}"
        assert (
            len(config[key]["components"]) > 0
        ), f"Expected at least one component in config entry for {key}"

import pytest
from components.plot_generator import find_component_by_id

# Sample plot configuration for testing
plot_configs = {
    "category1": {
        "components": [
            {
                "id": "component1",
                "type": "plot",
            },
            {
                "id": "component2",
                "type": "table",
            },
        ]
    },
    "category2": {
        "components": [
            {
                "id": "component3",
                "type": "UI",
            }
        ]
    },
}


def test_find_existing_component():
    """Test finding an existing component by ID."""
    component_id = "component2"
    expected_component = {
        "id": "component2",
        "type": "table",
    }
    component = find_component_by_id(component_id, plot_configs)
    assert (
        component == expected_component
    ), f"Expected {expected_component}, but got {component} for component_id '{component_id}'"


def test_find_another_existing_component():
    """Test finding another existing component by ID."""
    component_id = "component1"
    expected_component = {
        "id": "component1",
        "type": "plot",
    }
    component = find_component_by_id(component_id, plot_configs)
    assert (
        component == expected_component
    ), f"Expected {expected_component}, but got {component} for component_id '{component_id}'"


def test_find_nonexistent_component():
    """Test that a ValueError is raised when searching for a non-existent component."""
    component_id = "nonexistent"
    expected_error_message = f"Component with id '{component_id}' not found."
    with pytest.raises(ValueError, match=expected_error_message):
        find_component_by_id(component_id, plot_configs)


def test_empty_plot_configs():
    """Test that a ValueError is raised when the plot_configs dictionary is empty."""
    empty_plot_configs = {}
    component_id = "component1"
    with pytest.raises(
        ValueError, match=f"Component with id '{component_id}' not found."
    ):
        find_component_by_id(component_id, empty_plot_configs)

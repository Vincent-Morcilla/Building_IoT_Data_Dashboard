from components.plot_generator import find_component_by_id

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

# Find component with id 'component2'
component = find_component_by_id("component2", plot_configs)


def test_find_component_by_id():
    """Test the find_component_by_id function for expected behavior."""

    # Test for an existing component
    assert component == {
        "id": "component2",
        "type": "table",
    }, "The component does not match the expected result."

    # Test for a non-existent component
    try:
        find_component_by_id("nonexistent", plot_configs)
    except ValueError as error:
        assert (
            str(error) == "Component with id 'nonexistent' not found."
        ), "Expected ValueError with specific message for non-existent component."

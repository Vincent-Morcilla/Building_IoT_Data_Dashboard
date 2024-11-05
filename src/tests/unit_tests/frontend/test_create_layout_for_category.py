from components.plot_generator import create_layout_for_category

selected_plot_config = {
    "title": "Sample Category",
    "title_element": "H2",
    "components": [
        {
            "id": "test-plot",
            "type": "plot",
            "library": "px",
            "function": "line",
            "kwargs": {
                "x": [1, 2, 3],
                "y": [4, 1, 2],
            },
        },
        {
            "id": "test-input",
            "type": "UI",
            "element": "Input",
            "kwargs": {"type": "number", "value": 10},
            "label": "Enter a number:",
            "label_position": "above",
        },
    ],
}

# Call the function to generate layout components
layout_components = create_layout_for_category(selected_plot_config)


def test_create_layout_for_category():
    """Test if `create_layout_for_category` returns a valid layout component list.

    This test checks:
    - The output type is a list.
    - The length of the output list matches the expected number of components.
    """
    assert isinstance(layout_components, list), "Output should be a list."
    assert (
        len(layout_components) == 5
    ), "Output should contain five components: title, two elements, separator, and download button."

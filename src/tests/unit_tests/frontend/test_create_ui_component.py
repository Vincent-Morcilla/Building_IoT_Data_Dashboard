import pytest
from dash import dcc, html
from components.plot_generator import create_ui_component


# Define the input component
component_input = {
    "id": "test-input",
    "type": "UI",
    "element": "Input",
    "kwargs": {"type": "text", "placeholder": "Enter text..."},
    "label": "Test Input:",
    "label_position": "above",
}

# Create the input UI component
ui_component_input = create_ui_component(component_input)


def test_create_ui_component_input():
    """Test if create_ui_component returns an html.Div containing dcc.Input for input component."""
    # Ensure the outermost component is an html.Div
    assert isinstance(
        ui_component_input, html.Div
    ), "Expected html.Div for input component"

    # Access the inner html.Div (the first child)
    inner_div = ui_component_input.children[0]
    assert isinstance(inner_div, html.Div), "Expected inner component to be html.Div"

    # Access the list of children in the inner html.Div
    inner_children = inner_div.children
    assert len(inner_children) == 2, "Expected two children in inner html.Div"

    # The second child should be the dcc.Input
    input_component = inner_children[1]
    assert isinstance(
        input_component, dcc.Input
    ), "Expected dcc.Input as the second child of inner html.Div"


# Define the dropdown component
component_dropdown = {
    "id": "test-dropdown",
    "type": "UI",
    "element": "Dropdown",
    "kwargs": {
        "options": [
            {"label": "Option 1", "value": "1"},
            {"label": "Option 2", "value": "2"},
        ],
        "value": "1",
    },
    "label": "Select an Option:",
    "label_position": "next",
}

# Create the dropdown UI component
ui_component_dropdown = create_ui_component(component_dropdown)


def test_create_ui_component_dropdown():
    """Test if create_ui_component returns an html.Div containing dcc.Dropdown for dropdown component."""
    assert isinstance(
        ui_component_dropdown, html.Div
    ), "Expected html.Div for dropdown component"

    inner_div = ui_component_dropdown.children[0]
    assert isinstance(inner_div, html.Div), "Expected inner component to be html.Div"

    inner_children = inner_div.children
    assert len(inner_children) == 2, "Expected two children in inner html.Div"

    dropdown_component = inner_children[1]
    assert isinstance(
        dropdown_component, dcc.Dropdown
    ), "Expected dcc.Dropdown as the second child of inner html.Div"


def test_create_ui_component_invalid_element():
    """Test if create_ui_component raises ValueError for unsupported UI element type."""
    component_invalid = {
        "id": "test-invalid",
        "type": "UI",
        "element": "NonExistentComponent",  # Invalid component
        "kwargs": {},
    }
    with pytest.raises(ValueError) as exc_info:
        create_ui_component(component_invalid)
    assert "Unsupported UI element type" in str(exc_info.value)


# Define the component without a label
component_without_label = {
    "id": "test-no-label",
    "type": "UI",
    "element": "Input",
    "kwargs": {"type": "number", "placeholder": "Enter a number..."},
    # No 'label' key provided
}

# Create the UI component
ui_component_no_label = create_ui_component(component_without_label)


def test_create_ui_component_no_label():
    """Test if create_ui_component handles components without a label."""
    # The returned component should be an html.Div
    assert isinstance(
        ui_component_no_label, html.Div
    ), "Expected ui_component_no_label to be an instance of html.Div"

    # The Div should contain a single child, which is the ui_element
    children = ui_component_no_label.children
    assert len(children) == 1, "Expected one child in ui_component_no_label"
    ui_element = children[0]
    assert isinstance(
        ui_element, dcc.Input
    ), "Expected the child of ui_component_no_label to be an instance of dcc.Input"


# Define the component with a title
component_with_title = {
    "id": "test-with-title",
    "type": "UI",
    "element": "Slider",
    "kwargs": {"min": 0, "max": 10, "step": 1, "value": 5},
    "title": "Adjust the Slider",
    "title_element": "H3",
    "title_kwargs": {"style": {"color": "blue"}},
}

# Create the UI component
ui_component_with_title = create_ui_component(component_with_title)


def test_create_ui_component_with_title():
    """Test if create_ui_component correctly handles components with a title."""
    assert isinstance(
        ui_component_with_title, html.Div
    ), "Expected ui_component_with_title to be an instance of html.Div"

    # The first child should be the title component
    title_component = ui_component_with_title.children[0]
    assert isinstance(
        title_component, html.H3
    ), "Expected title to be an instance of html.H3"
    assert title_component.children == "Adjust the Slider", "Title text does not match"

    # The second child should be the ui_element
    ui_element = ui_component_with_title.children[1]
    assert isinstance(
        ui_element, dcc.Slider
    ), "Expected ui_element to be an instance of dcc.Slider"

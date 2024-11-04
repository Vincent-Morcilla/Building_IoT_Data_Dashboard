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

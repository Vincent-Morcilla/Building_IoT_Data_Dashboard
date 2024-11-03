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
    assert isinstance(
        ui_component_input, html.Div
    ), "Expected html.Div for input component"
    assert isinstance(
        ui_component_input.children[1], dcc.Input
    ), "Expected dcc.Input as child of input component"


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
    assert isinstance(
        ui_component_dropdown.children[1], dcc.Dropdown
    ), "Expected dcc.Dropdown as child of dropdown component"

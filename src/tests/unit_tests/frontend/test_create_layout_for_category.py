import pytest
from dash import html, dcc
from components.plot_generator import create_layout_for_category


# Positive test configuration
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
        {"type": "separator"},
        {"type": "placeholder", "id": "test-placeholder", "css": {"height": "100px"}},
    ],
}

layout_components = create_layout_for_category(selected_plot_config)


def test_create_layout_for_category_positive():
    """Test `create_layout_for_category` with a valid configuration."""

    # Validate overall structure
    assert isinstance(layout_components, list), "Output should be a list."
    expected_count = (
        len(selected_plot_config["components"]) + 1
    )  # title + one separator
    assert (
        len(layout_components) == expected_count
    ), f"Expected {expected_count} components, got {len(layout_components)}."

    # Validate title component
    title_component = layout_components[0]
    assert isinstance(title_component, html.H2), "Title should be of type `H2`."
    assert (
        title_component.children == selected_plot_config["title"]
    ), "Title text mismatch."

    # Validate plot component
    plot_component = layout_components[1]
    assert isinstance(
        plot_component, dcc.Graph
    ), "Plot component should be a Dash `dcc.Graph`."
    assert plot_component.id == "test-plot", "Plot component ID mismatch."

    # Validate UI component
    ui_component = layout_components[2]
    assert isinstance(
        ui_component, html.Div
    ), "UI component should be wrapped in `html.Div`."

    # Check the nested structure of ui_component
    if isinstance(ui_component.children, list) and len(ui_component.children) == 1:
        nested_div = ui_component.children[0]
        assert isinstance(
            nested_div, html.Div
        ), "Expected a nested Div wrapping label and input components."

        # Now, within nested_div, check for the Label and Input components
        if isinstance(nested_div.children, list) and len(nested_div.children) == 2:
            label_component, input_component = nested_div.children
            assert isinstance(
                label_component, html.Label
            ), "First child should be a `Label`."
            assert isinstance(
                input_component, dcc.Input
            ), "Second child should be `dcc.Input`."
            assert (
                input_component.type == "number"
            ), "UI component type should be `number`."
            assert input_component.id == "test-input", "UI component ID mismatch."
        else:
            raise AssertionError(
                "Nested Div structure is unexpected or missing children."
            )
    else:
        raise AssertionError(
            "UI component children structure is unexpected or missing."
        )

    # Validate separator
    separator_component = layout_components[3]
    assert isinstance(
        separator_component, html.Hr
    ), "Separator component should be `html.Hr`."

    # Validate placeholder component
    placeholder_component = layout_components[4]
    assert isinstance(
        placeholder_component, html.Div
    ), "Placeholder should be `html.Div`."
    assert placeholder_component.id == "test-placeholder", "Placeholder ID mismatch."
    assert (
        placeholder_component.style["height"] == "100px"
    ), "Placeholder style mismatch."


def test_create_layout_for_category_missing_title():
    """Test `create_layout_for_category` with missing title configuration."""
    config = selected_plot_config.copy()
    config.pop("title", None)  # Remove title to test handling of missing title

    layout = create_layout_for_category(config)
    title_component = layout[0]
    assert isinstance(title_component, html.H2), "Title element should default to `H2`."
    assert (
        title_component.children == ""
    ), "Title text should be empty when title is missing."


def test_create_layout_for_category_invalid_component_type():
    """Test `create_layout_for_category` with an invalid component type."""
    config = selected_plot_config.copy()
    config["components"].append({"type": "invalid_type"})

    with pytest.raises(ValueError, match="Unsupported component type 'invalid_type'"):
        create_layout_for_category(config)


def test_create_layout_for_category_missing_component_id():
    """Test `create_layout_for_category` with a plot component missing an ID."""
    config = selected_plot_config.copy()
    config["components"][0].pop("id", None)  # Remove ID from the plot component

    with pytest.raises(ValueError, match="Plot component must have an 'id' field."):
        create_layout_for_category(config)


def test_create_layout_for_category_missing_dataframe_in_table():
    """Test `create_layout_for_category` when a table component is missing a dataframe."""
    config = {
        "title": "Test Missing DataFrame",
        "title_element": "H2",
        "components": [
            {"type": "table", "id": "test-table", "title": "Missing DataFrame Table"}
        ],
    }

    with pytest.raises(ValueError, match="Table component requires a 'dataframe'."):
        create_layout_for_category(config)

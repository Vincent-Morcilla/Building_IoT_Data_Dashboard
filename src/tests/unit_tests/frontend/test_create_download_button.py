import pytest
from dash import dcc, html
import dash_bootstrap_components as dbc
from components.download_button import create_global_download_button


def test_create_global_download_button_structure():
    """
    Test the structure and properties of the global download button component.

    Ensures that the create_global_download_button function returns an html.Div
    containing a button, download component, and feedback div with the correct
    properties, IDs, and classes.
    """
    component = create_global_download_button()

    # Verify the top-level component is an html.Div with the correct ID and className
    assert isinstance(
        component, html.Div
    ), "Expected top-level component to be html.Div"
    assert component.id == "global-download-container", "Top-level Div ID is incorrect"
    assert (
        "d-grid gap-2 d-md-flex justify-content-md-end" in component.className
    ), "Top-level Div className is incorrect"

    # Verify that the container has three children: button, download component, feedback div
    children = component.children
    assert (
        len(children) == 3
    ), "Expected 3 children: button, download component, feedback div"

    # Test properties of the download button
    button = children[0]
    assert isinstance(button, dbc.Button), "First child should be a dbc.Button"
    assert button.id == "global-download-button", "Button ID is incorrect"
    assert (
        button.children == "Download All Visualisation Data as CSVs"
    ), "Button text is incorrect"
    assert button.className == "download-button", "Button className is incorrect"
    assert button.outline is True, "Button outline should be True"
    assert button.color == "success", "Button color should be 'success'"
    assert button.size == "lg", "Button size should be 'lg'"

    # Test properties of the download component
    download_component = children[1]
    assert isinstance(
        download_component, dcc.Download
    ), "Second child should be a dcc.Download"
    assert (
        download_component.id == "global-download-data"
    ), "Download component ID is incorrect"

    # Test properties of the feedback div
    feedback_div = children[2]
    assert isinstance(feedback_div, html.Div), "Third child should be an html.Div"
    assert feedback_div.id == "download-feedback", "Feedback div ID is incorrect"


@pytest.mark.parametrize(
    "component_index, expected_type, expected_id",
    [
        (0, dbc.Button, "global-download-button"),
        (1, dcc.Download, "global-download-data"),
        (2, html.Div, "download-feedback"),
    ],
)
def test_create_global_download_button_components(
    component_index, expected_type, expected_id
):
    """
    Parametrized test to verify the type and ID of each component in the global download container.

    Args:
        component_index (int): Index of the child component within the container's children list.
        expected_type (Type): Expected type of the component (e.g., dbc.Button, dcc.Download).
        expected_id (str): Expected ID of the component.

    Ensures that each component within the global download container matches the expected type
    and has the correct ID.
    """
    component = create_global_download_button()
    child = component.children[component_index]

    # Verify the type and ID of each child component
    assert isinstance(
        child, expected_type
    ), f"Expected type {expected_type} at index {component_index}"
    assert (
        child.id == expected_id
    ), f"Expected ID '{expected_id}' at index {component_index}"

import pytest
from dash import html, dcc
import dash_bootstrap_components as dbc
from components.layout import create_layout, home_page_content

def test_create_layout():
    # Test that the layout is created correctly
    layout = create_layout({})

    # Assert that the layout is a Dash `html.Div` component
    assert isinstance(layout, html.Div)

    # Assert that the layout has children (which are the elements inside the `html.Div`)
    assert hasattr(layout, 'children')
    assert isinstance(layout.children, list)  # Children should be a list of components

    # Check that the first child is a `dcc.Location` component
    assert isinstance(layout.children[0], dcc.Location)

    # Check that the second child is the sidebar (which is an `html.Div`)
    assert isinstance(layout.children[1], html.Div)

    # Check that the third child is a `dbc.Spinner` component
    assert isinstance(layout.children[2], dbc.Spinner)


def test_home_page_content():
    # Call the home_page_content function
    content = home_page_content()

    # Check if it returns a Div
    assert isinstance(content, html.Div)

    # Check the structure of the Div
    children = content.children
    assert isinstance(children, list)
    assert len(children) == 2  # Expecting an image and a paragraph

    # Check that the first child is an image with the correct attributes
    img = children[0]
    assert isinstance(img, html.Img)
    assert img.src == "/assets/title-logo.svg"
    assert img.alt == "Title-Logo"
    assert img.className == "title-logo"

    # Check that the second child is a paragraph with the correct content
    p = children[1]
    assert isinstance(p, html.P)
    assert p.children == "Select an option from the sidebar categories"

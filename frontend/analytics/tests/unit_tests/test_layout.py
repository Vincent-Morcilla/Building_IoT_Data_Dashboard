import pytest
from components.layout import create_layout
from dash import html, dcc
import dash_bootstrap_components as dbc

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

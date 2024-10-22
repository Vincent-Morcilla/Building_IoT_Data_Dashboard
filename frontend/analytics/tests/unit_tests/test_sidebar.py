import pytest
from components.sidebar import generate_sidebar
from dash import html
import dash_bootstrap_components as dbc

def test_generate_sidebar():
    # Test with a category (no subcategory needed as per previous request)
    categories = {"Main Category": []}
    sidebar = generate_sidebar(categories)

    # The sidebar is an `html.Div` object, so we need to check its children

    # Assert that the first link is "Home" with the correct href
    nav_links = sidebar.children[2].children  # This points to `dbc.Nav`

    # Check for 'Home' link
    assert isinstance(nav_links[0], dbc.NavLink)
    assert nav_links[0].children == "Home"
    assert nav_links[0].href == "/"

    # Check that 'Main Category' link is present with the correct href
    assert isinstance(nav_links[1], dbc.NavLink)
    assert nav_links[1].children == "Main Category"
    assert nav_links[1].href == "/main-category"
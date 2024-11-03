import pytest
from components.sidebar import generate_sidebar
import dash_bootstrap_components as dbc


def test_generate_sidebar():
    """
    Test the `generate_sidebar` function to ensure it generates the correct
    sidebar layout based on the provided category structure.
    """
    # Define categories with one main category and no subcategories
    categories = {"Main Category": []}

    # Generate the sidebar using the function to be tested
    sidebar = generate_sidebar(categories)

    # The sidebar should be an `html.Div` object with structured children
    # Access the navigation links inside the sidebar
    nav_links = sidebar.children[2].children  # This points to `dbc.Nav`

    # Assert that the first link is "Home" with the correct href
    assert isinstance(nav_links[0], dbc.NavLink), "First item should be a NavLink"
    assert nav_links[0].children == "Home", "First NavLink text should be 'Home'"
    assert nav_links[0].href == "/", "First NavLink href should be '/'"

    # Assert that 'Main Category' link is present with the correct href
    assert isinstance(nav_links[1], dbc.NavLink), "Second item should be a NavLink"
    assert (
        nav_links[1].children == "Main Category"
    ), "Second NavLink text should be 'Main Category'"
    assert (
        nav_links[1].href == "/main-category"
    ), "Second NavLink href should be '/main-category'"

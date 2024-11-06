import pytest
from dash import html
import dash_bootstrap_components as dbc
from components.sidebar import generate_sidebar


@pytest.fixture
def categories():
    """Fixture providing a sample categories dictionary."""
    return {"Main Category": [], "Extra Category": []}


@pytest.fixture
def sidebar(categories):
    """Fixture to generate a sidebar from the `generate_sidebar` function."""
    return generate_sidebar(categories)


def test_sidebar_is_html_div(sidebar):
    """Test if the generated sidebar is an instance of `html.Div`."""
    assert isinstance(
        sidebar, html.Div
    ), "The sidebar should be an instance of `html.Div`."


def test_sidebar_has_logo(sidebar):
    """Test if the sidebar includes a button with a logo."""
    # Ensure the first child is a button and contains the logo container
    logo_button = sidebar.children[0]
    assert isinstance(
        logo_button, html.Button
    ), "The first child should be a `html.Button`."

    # Check if the button contains the logo container
    logo_container = logo_button.children
    assert isinstance(
        logo_container, html.Div
    ), "Button's child should be a `html.Div` (logo container)."

    # Check if the logo container has an image with the correct source
    logo_image = logo_container.children
    assert isinstance(
        logo_image, html.Img
    ), "Logo container should contain an `html.Img`."

    # Validate the logo image source
    assert (
        logo_image.src == "/assets/logo.svg"
    ), "Logo src should be '/assets/logo.svg'."


def test_sidebar_includes_home_link(sidebar):
    """Test if the sidebar includes a 'Home' link at the top with correct href."""
    nav_links = sidebar.children[2].children  # `dbc.Nav` is the third child
    home_link = nav_links[0]

    assert isinstance(home_link, dbc.NavLink), "First item should be a NavLink."
    assert home_link.children == "Home", "First NavLink text should be 'Home'."
    assert home_link.href == "/", "First NavLink href should be '/'."


def test_sidebar_includes_category_links(categories, sidebar):
    """Test if the sidebar includes all provided categories as navigation links."""
    nav_links = sidebar.children[2].children

    # Start at index 1 since index 0 is the "Home" link
    for idx, (category, _) in enumerate(categories.items(), start=1):
        if category != "Home":  # Skip "Home" as it's added separately
            category_link = nav_links[idx]
            assert isinstance(
                category_link, dbc.NavLink
            ), f"Item {idx} should be a NavLink."
            assert (
                category_link.children == category
            ), f"NavLink text should be '{category}'."
            expected_href = f"/{category.lower().replace(' ', '-')}"
            assert (
                category_link.href == expected_href
            ), f"NavLink href should be '{expected_href}'."


def test_sidebar_structure(sidebar):
    """Test the overall structure of the sidebar."""
    assert len(sidebar.children) == 3, "Sidebar should have 3 main children elements."
    assert isinstance(
        sidebar.children[1], html.Hr
    ), "Second child should be an `html.Hr` separator."
    assert isinstance(
        sidebar.children[2], dbc.Nav
    ), "Third child should be a `dbc.Nav` element."


def test_nav_pills_and_vertical_property(sidebar):
    """Test if the `dbc.Nav` component in the sidebar has `pills` and `vertical` properties enabled."""
    nav_component = sidebar.children[2]
    assert nav_component.vertical is True, "`dbc.Nav` should have `vertical=True`."
    assert nav_component.pills is True, "`dbc.Nav` should have `pills=True`."

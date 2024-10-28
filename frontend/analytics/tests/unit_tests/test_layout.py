import pytest
from dash import html, dcc
import dash_bootstrap_components as dbc
from components.layout import create_layout, home_page_content
from helpers.helpers import create_category_structure

@pytest.fixture
def mock_plot_configs():
    """Fixture providing mock plot configurations for testing."""
    return {
        ("Category1", "Subcategory1"): {"title": "Example Plot", "components": []},
        ("Category2", None): {"title": "Another Plot", "components": []},
    }

@pytest.fixture
def mock_categories_structure(mock_plot_configs):
    """Fixture generating mock category structure from plot configurations."""
    return create_category_structure(mock_plot_configs.keys())

def test_create_layout(mock_plot_configs, mock_categories_structure):
    """
    Test that create_layout correctly returns an html.Div layout 
    with expected components and structure.
    """
    layout = create_layout(mock_plot_configs, mock_categories_structure)

    assert isinstance(layout, html.Div), "The layout should be an `html.Div` component."
    assert hasattr(layout, 'children') and isinstance(layout.children, list), (
        "The layout should have a list of child components."
    )
    
    # Test main components within layout children
    assert isinstance(layout.children[0], dcc.Location), (
        "First child should be `dcc.Location` for URL path handling."
    )

    sidebar = layout.children[1]
    assert isinstance(sidebar, html.Div), "Second child should be an `html.Div` sidebar."
    assert "sidebar" in sidebar.className, "Sidebar `html.Div` should have `sidebar` as a class name."

    spinner = layout.children[2]
    assert isinstance(spinner, dbc.Spinner), "Third child should be `dbc.Spinner` for loading states."
    assert isinstance(spinner.children[0], html.Div), (
        "Spinner should contain an `html.Div` for content display area."
    )
    assert spinner.children[0].id == "page-content", (
        "The content `html.Div` should have id 'page-content'."
    )

def test_home_page_content():
    """
    Test that home_page_content returns the correct structure 
    with specific children components.
    """
    content = home_page_content()

    assert isinstance(content, html.Div), "home_page_content should return an `html.Div`."
    assert hasattr(content, 'children') and isinstance(content.children, list), (
        "The returned Div should have a list of child components."
    )

    # Check that the structure has two children as expected: an image and a paragraph
    assert len(content.children) == 2, "The content `html.Div` should have exactly two children."

    img, paragraph = content.children[0], content.children[1]
    assert isinstance(img, html.Img), "First child should be an `html.Img` for the logo."
    assert img.src == "/assets/title-logo.svg", "The image source should be `/assets/title-logo.svg`."
    assert img.alt == "Title-Logo", "The image `alt` attribute should be 'Title-Logo'."
    assert img.className == "title-logo", "The image should have the class `title-logo`."

    assert isinstance(paragraph, html.P), "Second child should be an `html.P` for the prompt message."
    assert paragraph.children == "Select an option from the sidebar categories", (
        "Paragraph should contain the prompt 'Select an option from the sidebar categories'."
    )

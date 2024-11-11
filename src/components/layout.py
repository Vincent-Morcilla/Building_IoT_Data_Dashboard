"""
Defines the main layout of the Dash application.

This module sets up the overall structure of the app, including the sidebar,
content area, and URL routing for page navigation. It includes functions to create
the app's layout and the home page content, integrating the sidebar and content
components into a cohesive user interface.
"""

from dash import dcc, html
import dash_bootstrap_components as dbc

from components.sidebar import generate_sidebar
from models.types import CategoriesStructure


def create_layout(categories_structure: CategoriesStructure) -> html.Div:
    """
    Create the main layout for the Dash application.

    The layout includes a sidebar for navigation, a content area where specific
    page content will be displayed, and URL routing for page navigation.

    Args:
        categories_structure (CategoriesStructure): Contains categories, category key mapping,
                                                    and subcategory key mapping.

    Returns:
        html.Div: A Div component representing the overall layout of the application.
    """
    # Unpack the categories structure
    categories, category_key_mapping, subcategory_key_mapping = categories_structure

    # Generate the sidebar
    sidebar = generate_sidebar(categories)

    # Define the content area where page-specific content will be displayed
    content = html.Div(id="page-content", className="content")

    # Return the layout structure
    return html.Div(
        [
            dcc.Location(id="url"),  # To capture the current URL
            sidebar,  # Sidebar with categories
            dbc.Spinner(
                children=[content],  # Main content area
                color="#3c9639",
                fullscreen=False,
                type="border",
                size="md",
            ),
        ]
    )


def home_page_content(categories_structure: CategoriesStructure) -> html.Div:
    """
    Create the homepage content for the Dash application.

    Args:
        categories_structure (CategoriesStructure): Contains categories, category key mapping,
        and subcategory key mapping.

    Returns:
        html.Div: A Div component containing the homepage content.
    """
    # Check if there are categories available in categories_structure
    has_categories = categories_structure and categories_structure[0]

    return html.Div(
        [
            html.Img(
                src="/assets/title-logo.svg", alt="Title-Logo", className="title-logo"
            ),
            html.P(
                "Select an option from the sidebar categories"
                if has_categories
                else "No analyses could be run on the provided dataset"
            ),
        ],
        className="home-page-content",
    )

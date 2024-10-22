from dash import dcc, html
import dash_bootstrap_components as dbc
from components.sidebar import generate_sidebar
from helpers.helpers import create_category_structure, create_url_mapping

# Creates the main layout for the Dash application, which includes a sidebar, URL routing, and a content area.
def create_layout(plot_configs):
    # Create a mapping from URL-friendly paths to dataframe keys
    url_to_key_mapping = create_url_mapping(plot_configs)

    # Create categories and subcategories from the dataframe keys
    categories = create_category_structure(plot_configs.keys())

    # Generate the sidebar
    sidebar = generate_sidebar(categories)

    # Define the content area where page-specific content will be displayed
    content = html.Div(id="page-content", className="content")

    # Return the layout structure
    return html.Div(
        [
            dcc.Location(id="url"),  # To capture the current URL
            sidebar,
            dbc.Spinner(children=[content], color="#3c9639", fullscreen=False, type="border", size="md"),
        ]
    )

# Creates the Homepage content for the Dash application
def home_page_content():
    return html.Div(
        [
            html.Img(src="/assets/title-logo.svg", alt="Title-Logo", className="title-logo"),
            html.P("Select an option from the sidebar categories")
        ],
        className="home-page-content",
    )
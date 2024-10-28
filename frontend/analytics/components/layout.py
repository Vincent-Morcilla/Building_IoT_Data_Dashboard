from dash import dcc, html
import dash_bootstrap_components as dbc
from components.sidebar import generate_sidebar

def create_layout(plot_configs, categories_structure):
    """
    Create the main layout for the Dash application.

    The layout includes a sidebar for navigation, a content area where specific
    page content will be displayed, and URL routing for page navigation.

    Args:
        plot_configs (dict): The configuration for various plots used in the app.
        categories_structure (tuple): A tuple containing the categories, category key mapping,
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
                size="md"
            ),
        ]
    )


def home_page_content():
    """
    Create the homepage content for the Dash application.

    The homepage displays a logo and a message prompting users to select an option
    from the sidebar.

    Returns:
        html.Div: A Div component containing the homepage content.
    """
    return html.Div(
        [
            html.Img(src="/assets/title-logo.svg", alt="Title-Logo", className="title-logo"),
            html.P("Select an option from the sidebar categories")
        ],
        className="home-page-content",
    )

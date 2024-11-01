from dash import html
import dash_bootstrap_components as dbc

def generate_sidebar(categories):
    """
    Generate a sidebar for a Dash application with navigation links.

    Args:
        categories (dict): A dictionary where keys are category names and values are lists of subcategories.

    Returns:
        html.Div: A Dash HTML Div component representing the sidebar with navigation links.
    """
    nav_links = []

    # Add "Home" link at the top of the sidebar
    nav_links.append(dbc.NavLink("Home", href="/", active="exact"))

    # Loop through categories and generate navigation links
    for category, subcategories in categories.items():
        if category != "Home":  # Skip "Home" as it's already added
            nav_links.append(
                dbc.NavLink(
                    category,
                    href=f"/{category.lower().replace(' ', '-')}",
                    active="exact",
                )
            )

    # Return the sidebar structure as an HTML Div element
    return html.Div(
        [
            html.Button(
                html.Div(
                    html.Img(src="/assets/logo.svg", className="sidebar-logo"),
                    className="sidebar-logo-container",
                ),
                id="logo-button",
                n_clicks=0,
                className="logo-button"
            ),
            html.Hr(),
            dbc.Nav(nav_links, vertical=True, pills=True),
        ],
        className="sidebar",
    )

from dash import html
import dash_bootstrap_components as dbc

# Generates a sidebar for a Dash web application.
def generate_sidebar(categories):
    nav_links = []

    # Add "Home" link at the top of the sidebar
    nav_links.append(dbc.NavLink("Home", href="/", active="exact"))

    # Loop through categories and generate links
    for category, subcategories in categories.items():
        if category != "Home":  # Skip "Home" as it's already added
            nav_links.append(
                dbc.NavLink(
                    category,
                    href=f"/{category.lower().replace(' ', '-')}",
                    active="exact",
                )
            )

    return html.Div(
        [
            html.Button(
                html.Div(
                    html.Img(src="/assets/logo.svg", className="sidebar-logo"),
                    className="sidebar-logo-container",
                ),
                id="logo-button",
                n_clicks=0,
                className="logo-button",
                style={
                    "background": "none",
                    "border": "none",
                    "padding": "0",
                    "margin": "0",
                    "cursor": "pointer",
                },
            ),
            html.Hr(),
            dbc.Nav(nav_links, vertical=True, pills=True)
        ],
        className="sidebar",
    )

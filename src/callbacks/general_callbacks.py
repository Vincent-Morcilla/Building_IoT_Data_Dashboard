"""
Registers general callbacks for navigation and content rendering.

This module sets up callbacks that handle page navigation and content updates
based on the URL pathname. It manages the redirection to the home page when the
logo is clicked and updates the displayed content when users navigate between
different sections of the app.
"""

from typing import Any

from dash import Dash, Input, Output, html

from components.layout import home_page_content
from components.tabs import create_tab_layout
from models.types import PlotConfig, CategoriesStructure


def register_general_callbacks(
    app: Dash, plot_configs: PlotConfig, categories_structure: CategoriesStructure
) -> None:
    """
    Registers general callbacks for the Dash application.

    This function sets up two callbacks:
    1. Redirecting to the homepage when the logo is clicked.
    2. Displaying the appropriate page content based on the URL pathname.

    Args:
        app (Dash): Dash app instance where callbacks will be registered.
        plot_configs (PlotConfig): Configuration dictionary for plots and components.
        categories_structure (CategoriesStructure): A tuple containing the categories,
                              category key mapping, and subcategory key mapping.
    """
    # Unpack the categories structure
    categories, category_key_mapping, subcategory_key_mapping = categories_structure

    @app.callback(Output("url", "pathname"), Input("logo-button", "n_clicks"))
    def redirect_to_home(n_clicks: int) -> Any:
        """
        Redirects the user to the homepage when the logo button is clicked.

        Args:
            n_clicks (int): Number of times the logo button is clicked.

        Returns:
            str or None: The homepage URL ("/") if clicked, otherwise None.
        """
        if n_clicks and n_clicks > 0:
            return "/"
        return None

    @app.callback(Output("page-content", "children"), [Input("url", "pathname")])
    def display_page(pathname: str) -> html.Div:
        """
        Generates and returns the page content based on the current URL pathname.

        Args:
            pathname (str): The current URL pathname.

        Returns:
            dash.html.Div: The page content based on the selected category or a default message.
        """
        if pathname == "/" or pathname is None:
            return home_page_content(categories_structure)

        # Split the pathname to get category and subcategory
        path_parts = pathname.strip("/").split("/")
        selected_category = path_parts[0] if len(path_parts) > 0 else None

        if selected_category:
            return create_tab_layout(
                plot_configs,
                selected_category,
                categories,
                category_key_mapping,
                subcategory_key_mapping,
            )

        # Handle invalid URLs
        return html.Div(
            [
                html.H4(
                    f"No content available for {pathname.strip('/').replace('-', ' ')}. Please select an option from the sidebar."
                )
            ],
            id="page-content",
        )

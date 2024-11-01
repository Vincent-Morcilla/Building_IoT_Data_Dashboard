from dash import Dash
import dash_bootstrap_components as dbc
from components.layout import create_layout
from callbacks.download_button_callbacks import register_download_callbacks
from callbacks.general_callbacks import register_general_callbacks
from callbacks.plot_callbacks import register_plot_callbacks
from helpers.helpers import create_category_structure
from sampledata.plot_configs import plot_configs

# Name of the application
APP_NAME = "Network in Progress"


def create_app() -> Dash:
    """
    Initialize and configure the Dash application.

    Returns:
        Dash: Configured Dash application instance.
    """
    # Initialize the Dash app with Bootstrap styling
    app = Dash(
        __name__,
        external_stylesheets=[dbc.themes.BOOTSTRAP],
        suppress_callback_exceptions=True,
    )
    app.title = APP_NAME

    # Create category structure for tabs and retrieve mappings
    categories, category_key_mapping, subcategory_key_mapping = (
        create_category_structure(plot_configs.keys())
    )
    categories_structure = (categories, category_key_mapping, subcategory_key_mapping)

    # Set the app layout
    app.layout = create_layout(plot_configs, categories_structure)

    # Register download, general and plot-related callbacks
    register_download_callbacks(app, plot_configs)
    register_general_callbacks(app, plot_configs, categories_structure)
    register_plot_callbacks(app, plot_configs)

    return app


if __name__ == "__main__":
    # Run the Dash app server with debug mode enabled
    create_app().run_server(debug=True)

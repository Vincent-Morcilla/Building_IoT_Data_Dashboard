from dash import Dash
from data.plot_configs import plot_configs
import dash_bootstrap_components as dbc

# Importing layout and callbacks
from components.layout import create_layout
from callbacks.general_callbacks import general_callbacks
from helpers.helpers import create_category_structure

# Name of the application
APP_NAME = "Network in Progress"

# Initialize the app
app = Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP], suppress_callback_exceptions=True)
app.title = APP_NAME

# Set the layout
app.layout = create_layout(plot_configs)

# Create categories structure for tabs
categories_structure = create_category_structure(plot_configs.keys())

# Register callbacks
general_callbacks(app, categories_structure)

# Run the server
if __name__ == "__main__":
    app.run_server(debug=True)

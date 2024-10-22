from dash import Dash
from data.plot_configs import plot_configs
import dash_bootstrap_components as dbc

# Importing layout and callbacks
from components.layout import create_layout
from callbacks.general_callbacks import general_callbacks

# General configuration for the app
from config.app_config import APP_NAME

# Initialize the app
app = Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP], suppress_callback_exceptions=True)
app.title = APP_NAME

# Set the layout
app.layout = create_layout(plot_configs)

# Register callbacks
general_callbacks(app)

# Run the server
if __name__ == "__main__":
    app.run_server(debug=True)

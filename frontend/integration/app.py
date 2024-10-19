import dash
import dash_bootstrap_components as dbc
from dash import dcc, html
from pages.main_page import layout as main_page_layout  # Import the main page layout
from frontend.integration.pages.analytics_page import layout as analytics_page_layout  # Import the analytics page layout
from callbacks.main_callbacks import register_callbacks  # Import the callback registration function

# Initialize the app with Bootstrap CSS and suppress callback exceptions
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP], suppress_callback_exceptions=True)

# Enable multi-page support
server = app.server

# App layout with a container that switches between pages
app.layout = html.Div([
    dcc.Location(id='url', refresh=False),
    html.Div(id='page-content')  # This will dynamically update based on URL
])

# Register all callbacks
register_callbacks(app)  # Call the function that registers the callbacks

# Callback to control page routing
@app.callback(
    dash.dependencies.Output('page-content', 'children'),
    [dash.dependencies.Input('url', 'pathname'), dash.dependencies.Input('url', 'search')]
)
def display_page(pathname, search):
    if pathname == '/analytics':
        return analytics_page_layout(search)  # Call the function here
    else:
        return main_page_layout  # Default is the main page

if __name__ == "__main__":
    app.run_server(port=8050, debug=True)

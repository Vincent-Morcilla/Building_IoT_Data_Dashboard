import dash
import dash_bootstrap_components as dbc
from dash import dcc, html, Input, Output
from pages.main_page import main_page_layout  # Import the main page layout
from pages.analytics_page import analytics_layout  # Import the analytics page layout
from callbacks.main_callbacks import register_callbacks  # Import the main page callback registration function

# Name of app
APP_NAME = "Network in Progress"

# Initialize the app with Bootstrap CSS and suppress callback exceptions
app = dash.Dash(APP_NAME, external_stylesheets=[dbc.themes.BOOTSTRAP], suppress_callback_exceptions=True)

# App name displayed on tab
app.title = APP_NAME

# Enable multi-page support
server = app.server

# App layout with a container that switches between pages
app.layout = html.Div([
    dcc.Location(id='url', refresh=False),
    html.Div(id='page-content')  # This will dynamically update based on URL
])

# Register all callbacks
register_callbacks(app)  # Call the function that registers the main page callbacks

# Callback to control page routing
@app.callback(
    Output('page-content', 'children'),
    [Input('url', 'pathname')]
)
def display_page(pathname):
    if pathname == '/analytics':
        return analytics_layout() # Return the analytics page layout
    elif pathname == '/':
        return main_page_layout  # Return the main page layout
    else:
        return html.Div([html.H1("404 Page not found")])

if __name__ == "__main__":
    app.run_server(port=8050, debug=True)

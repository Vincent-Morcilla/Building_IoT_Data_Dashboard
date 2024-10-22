from dash import dcc
import dash_bootstrap_components as dbc

def create_tab_content(plot_type, plot_settings, plot_id, subcategory):
    # Always return a "Main" tab with a generic layout
    return dbc.Tab(
        dbc.Container(
            [
                dcc.Graph(id={"type": "main-graph", "index": plot_id}),
                None,  # No UI controls for now
            ]
        ),
        label="Main",  # Static label for the tab
        tab_id=plot_id,
    )

from dash import html
import dash_bootstrap_components as dbc
from components.layout_components import create_title_logo, create_dataset_form, create_progress_section

layout = dbc.Container(
    [
        create_title_logo(),
        html.Hr(),
        create_dataset_form(),
        create_progress_section(),
    ],
    fluid=True,
)

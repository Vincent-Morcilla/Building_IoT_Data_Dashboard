from dash import html, dcc
import dash_bootstrap_components as dbc
import os
import sys

# Add the root directory to the system path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../")))

# PipelineProgress class and pipeline functions defined
from backend.pipeline import get_pipeline_progress, data_pipeline

# Function to create the title-logo
def create_title_logo():
    return html.Div(
        [
            html.A(
                html.Img(src="/assets/title-logo.svg", className="title-logo", id="title"),
                href="/",
            )
        ],
        className="title-logo-container",
    )

# Function to create the form with input, file upload, and Analyse button
def create_dataset_form():
    return html.Div(
        [
            html.H5("Name your dataset"),
            dbc.Input(id="dataset-name-input", placeholder="Enter dataset name...", type="text"),
            html.Br(),
            html.H5("Upload at least one .ttl file and/or a mapping .csv file"),
            dcc.Upload(
                id="upload-data",
                children=dbc.Button("Upload Files", color="primary"),
                multiple=True,
                accept=".ttl, .csv",
            ),
            html.Div(id="file-names"),
            html.Br(),
            html.H5("Input path to zip file containing one or more .pkl files"),
            dbc.Input(id="zip-file-path", placeholder="Enter zip file path...", type="text"),
            html.Br(),
            dbc.Button("Screen Files", id="screen-files-button", color="primary", disabled=True),
            html.Div(id="screen-result"),
            html.Br(),
            dcc.Store(id="zip-validity-store"),
            dcc.Store(id="file-validity-store"),
            dbc.Button("Analyse", id="analyse-button", color="success", disabled=True, n_clicks=0),
        ],
        className="form-container",
        id="form-container",
    )

# Function to create the error modal
def create_error_modal():
    return dbc.Modal(
        [
            dbc.ModalHeader("Error"),
            dbc.ModalBody(id="error-message"),
            dbc.ModalFooter(
                dbc.Button("Close", id="close-error-modal", className="ml-auto", n_clicks=0)
            ),
        ],
        id="error-modal",
        centered=True,
        is_open=False
    )

# Function to create the progress section
def create_progress_section():
    return html.Div(
        [
            html.Div(id='status'),
            dbc.Progress(id='progress-bar', value=0, max=100, striped=True, animated=True),
            dcc.Interval(id='interval-component', interval=1*1000, n_intervals=0, disabled=True),
            dcc.Store(id='session-id-store'),
            create_error_modal() 
        ],
        id='progress-section',
        style={'display': 'none'}
    )

# Main page layout
def main_page():
    return dbc.Container(
        [
            create_title_logo(),
            html.Hr(),
            create_dataset_form(),
            create_progress_section(),
        ],
        fluid=True,
    )

# Analytics page layout
def analytics_page(session_id):
    if session_id:
        progress = get_pipeline_progress(session_id)
        if progress:
            analytics = progress.get_analytics()
            if analytics:
                modified_file_names = analytics.get('file_names', [])
                dataset_name = analytics.get('dataset_name', '')
                uploaded_files = analytics.get('uploaded_files', [])
                return html.Div([
                    create_title_logo(),
                    html.Hr(),
                    html.H2("Analytics"),
                    html.H3(f"Dataset Name: {dataset_name}"),
                    html.H4("Uploaded Files:"),
                    html.Ul([html.Li(name) for name in uploaded_files]),
                    html.H4("Modified Filenames:"),
                    html.Ul([html.Li(name) for name in modified_file_names]),
                ])
    return html.Div([
        create_title_logo(),
        html.Hr(),
        html.H2("Analytics"),
        html.P("No analytics available.")
    ])

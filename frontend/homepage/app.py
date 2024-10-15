import dash
from dash import html, dcc, Output, Input, State, callback_context
import dash_bootstrap_components as dbc
import zipfile
import os
import threading
import sys
from dash.exceptions import PreventUpdate
from urllib.parse import parse_qs

# Add the root directory to the system path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))

# PipelineProgress class and pipeline functions defined
from backend.pipeline import get_pipeline_progress, data_pipeline

# Initialize the app with Bootstrap CSS and suppress callback exceptions
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP], suppress_callback_exceptions=True)

# Enable multi-page support
server = app.server

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
            # Instruction
            html.H5("Name your dataset"),
            # Input field for dataset name
            dbc.Input(id="dataset-name-input", placeholder="Enter dataset name...", type="text"),
            html.Br(),
            # Instruction for file upload
            html.H5("Upload at least one .ttl file and/or a mapping .csv file"),
            # File upload button
            dcc.Upload(
                id="upload-data",
                children=dbc.Button("Upload Files", color="primary"),
                multiple=True,  # Allow multiple file uploads
                accept=".ttl, .csv",  # Restrict file types
            ),
            # Display file names and error messages
            html.Div(id="file-names"),
            html.Br(),
            # Input field for zip file path
            html.H5("Input path to zip file containing one or more .pkl files"),
            dbc.Input(id="zip-file-path", placeholder="Enter zip file path...", type="text"),
            html.Br(),
            # Screen button for zip file
            dbc.Button("Screen Files", id="screen-files-button", color="primary", disabled=True),
            html.Div(id="screen-result"),
            html.Br(),
            # Hidden store to manage file screening result for zip
            dcc.Store(id="zip-validity-store"),
            # Hidden store to manage file screening result for uploads
            dcc.Store(id="file-validity-store"),
            # Analyse button, initially disabled
            dbc.Button("Analyse", id="analyse-button", color="success", disabled=True, n_clicks=0),
        ],
        className="form-container",
        id="form-container",  # Add an id to the form container for styling
    )

# Function to create the error modal (no need to include in homepage)
def create_error_modal():
    return dbc.Modal(
        [
            dbc.ModalHeader("Error"),
            dbc.ModalBody(id="error-message"),  # Body where the error message will be shown
            dbc.ModalFooter(
                dbc.Button("Close", id="close-error-modal", className="ml-auto", n_clicks=0)
            ),
        ],
        id="error-modal",
        centered=True,
        is_open=False  # Modal is initially closed
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
        style={'display': 'none'}  # Hidden initially
    )

# Function to create the results page
def results_page(session_id):
    if session_id:
        progress = get_pipeline_progress(session_id)
        if progress:
            results = progress.get_results()
            if results:
                modified_file_names = results.get('file_names', [])
                dataset_name = results.get('dataset_name', '')
                uploaded_files = results.get('uploaded_files', [])
                return html.Div([
                    create_title_logo(),
                    html.Hr(),
                    html.H2("Results"),
                    html.H3(f"Dataset Name: {dataset_name}"),
                    html.H4("Uploaded Files:"),
                    html.Ul([html.Li(name) for name in uploaded_files]),
                    html.H4("Modified Filenames:"),
                    html.Ul([html.Li(name) for name in modified_file_names]),
                ])
    # If session_id is not valid or no results
    return html.Div([
        create_title_logo(),
        html.Hr(),
        html.H2("Results"),
        html.P("No results available.")
    ])

# Page layout with only the logo-title at the top
def main_page():
    return dbc.Container(
        [
            create_title_logo(),  # Add the title-logo at the top
            html.Hr(),  # Horizontal line for separation
            create_dataset_form(),  # Add the form underneath the logo
            create_progress_section(),  # Add the progress section
        ],
        fluid=True,
    )

# App layout with a container that switches between pages
app.layout = html.Div([
    dcc.Location(id='url', refresh=False),
    html.Div(id='page-content')
])

# Update page content based on the current pathname
@app.callback(
    Output('page-content', 'children'),
    Input('url', 'pathname'),
    Input('url', 'search')
)
def display_page(pathname, search):
    if pathname == '/results':
        # Extract session_id from search parameters
        query_params = parse_qs(search.lstrip('?'))
        session_id = query_params.get('session_id', [None])[0]
        return results_page(session_id)
    else:
        return main_page()

# Callback to handle file uploads, screen them, and store the result
@app.callback(
    Output('file-names', 'children'),
    Output('file-validity-store', 'data'),  # Store file validity status (True/False)
    Input('upload-data', 'contents'),
    State('upload-data', 'filename')
)
def screen_uploaded_files(file_contents, file_names):
    if file_contents is None:
        # No files uploaded yet
        return "", False

    ttl_files = [f for f in file_names if f.endswith('.ttl')]
    csv_files = [f for f in file_names if f.endswith('.csv')]

    # Screening logic for valid file combinations
    if (len(ttl_files) == 1 and len(csv_files) <= 1) or (len(ttl_files) == 2 and len(csv_files) <= 1):
        # Files are valid, return file names and set file validity to True
        uploaded_files = ", ".join(file_names)
        return f"Uploaded files: {uploaded_files}", True
    else:
        # Invalid file selection, show error and set file validity to False
        return "Error: Please upload 1 or 2 .ttl files and at most 1 .csv file.", False

# Callback to toggle 'Screen Files' button based on zip file path input
@app.callback(
    Output('screen-files-button', 'disabled'),
    Input('zip-file-path', 'value')
)
def toggle_screen_button(zip_file_path):
    return not zip_file_path or not zip_file_path.strip()

# Callback to screen zip file for .pkl files
@app.callback(
    Output('screen-files-button', 'children'),
    Output('screen-result', 'children'),
    Output('zip-validity-store', 'data'),  # Store zip file validity (True/False)
    Input('screen-files-button', 'n_clicks'),
    State('zip-file-path', 'value'),
    prevent_initial_call=True
)
def screen_zip_file(n_clicks, zip_file_path):
    # Ensure the file exists at the provided path
    if not os.path.exists(zip_file_path):
        return "Screen Files", f"Error: File not found at {zip_file_path}.", False

    try:
        # Try opening the zip file
        with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
            pkl_files = [f for f in zip_ref.namelist() if f.endswith('.pkl')]

        if not pkl_files:
            # No .pkl files found
            return "Screen Files", "Error: No .pkl files found.", False
        else:
            # Successfully found .pkl files
            return "Screen Files", f"{len(pkl_files)} .pkl file(s) found.", True

    except Exception as e:
        # Handle zip file read errors
        return "Screen Files", f"Error: Could not read zip file. {str(e)}", False

# Callback to manage enabling/disabling of the Analyse button
@app.callback(
    Output('analyse-button', 'disabled'),
    Input('dataset-name-input', 'value'),
    Input('file-validity-store', 'data'),  # Check stored file validity
    Input('zip-validity-store', 'data')  # Check stored zip file validity
)
def toggle_analyse_button(dataset_name, file_validity, zip_validity):
    # Enable Analyse button only if both the dataset name, uploaded files, and zip file are valid
    if dataset_name and dataset_name.strip() and file_validity and zip_validity:
        return False  # Enable button
    return True  # Disable button otherwise

# Callback to handle the Analyse button click and start the pipeline
@app.callback(
    [Output('form-container', 'style'),
     Output('progress-section', 'style'),
     Output('interval-component', 'disabled'),
     Output('session-id-store', 'data')],
    [Input('analyse-button', 'n_clicks')],
    [State('dataset-name-input', 'value'),
     State('upload-data', 'contents'),
     State('upload-data', 'filename'),
     State('zip-file-path', 'value')]
)
def start_pipeline(n_clicks, dataset_name, uploaded_files_contents, uploaded_files_names, zip_file_path):
    if n_clicks > 0:
        # Hide the form
        form_style = {'display': 'none'}
        # Show the progress section
        progress_style = {'display': 'block'}
        # Enable the interval component
        interval_disabled = False

        # Process the uploaded files
        processed_files = []
        if uploaded_files_contents and uploaded_files_names:
            for content, name in zip(uploaded_files_contents, uploaded_files_names):
                processed_files.append({
                    'filename': name,
                    'content': content
                })

        # Prepare data for the pipeline
        data = {
            'dataset_name': dataset_name,
            'uploaded_files': processed_files,
            'zip_file_path': zip_file_path
        }

        # Generate a session ID
        session_id = f"session_{n_clicks}"

        # Start the pipeline in a new thread
        thread = threading.Thread(target=data_pipeline, args=(data, session_id), daemon=True)
        thread.start()

        # Return the session ID
        return form_style, progress_style, interval_disabled, session_id

    raise PreventUpdate

# Combine the callback for progress update and modal close into one
@app.callback(
    [Output('status', 'children'),
     Output('progress-bar', 'value'),
     Output('progress-bar', 'label'),
     Output('url', 'pathname'),
     Output('url', 'search'),
     Output('error-message', 'children'),
     Output('error-modal', 'is_open')],
    [Input('interval-component', 'n_intervals'),
     Input('close-error-modal', 'n_clicks')],
    [State('session-id-store', 'data')]
)
def update_progress_or_close_modal(n_intervals, close_clicks, session_id):
    # Use dash.callback_context to determine which input triggered the callback
    ctx = callback_context
    if not ctx.triggered:
        raise PreventUpdate

    trigger = ctx.triggered[0]['prop_id'].split('.')[0]

    if trigger == 'interval-component':
        # Progress bar logic
        if session_id:
            progress = get_pipeline_progress(session_id)
            if progress:
                progress_data = progress.get_progress()
                stage = progress_data.get('stage', 'Unknown')
                percentage = progress_data.get('percentage', 0)
                error = progress_data.get('error')

                if error:
                    # Trigger the modal to show the error and return the error message
                    return f"Error: {error}", 0, "", dash.no_update, dash.no_update, error, True

                if percentage == 100:
                    # Redirect to results page with session ID
                    return f"Stage: {stage}", percentage, f"{percentage}%", '/results', f"?session_id={session_id}", dash.no_update, False

                return f"Stage: {stage}", percentage, f"{percentage}%", dash.no_update, dash.no_update, dash.no_update, False

    elif trigger == 'close-error-modal':
        # Modal close logic - redirect to homepage
        if close_clicks > 0:
            return dash.no_update, dash.no_update, dash.no_update, '/', dash.no_update, dash.no_update, False

    raise PreventUpdate

# Run the app
if __name__ == "__main__":
    app.run_server(port=8050, debug=True)

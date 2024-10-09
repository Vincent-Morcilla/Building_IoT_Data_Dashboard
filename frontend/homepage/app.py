import base64
import io
import zipfile
import dash
from dash import dcc, html
from dash.dependencies import Input, Output, State
import dash_bootstrap_components as dbc

# Initialize the app with a Bootstrap theme
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])

# Set tab name
app.title = "Network in Progress"

# App layout
app.layout = dbc.Container(
    [
        # Title and logo
        dbc.Row(
            [
                html.A(
                    html.Img(
                        src="/assets/title-logo.svg",
                        id="title",
                        className="img-fluid"
                    ),
                    href="/"
                ),
            ],
            justify="center",
        ),
        # Instruction text and file upload
        dbc.Row(
            html.Div(
                [
                    html.P(
                        "Upload a .pkl and .ttl file or a compressed file containing both",
                        id="instructions"
                    ),
                    dcc.Upload(
                        id='upload',
                        children=html.Div(
                            ['Drag and Drop or ', html.B('Select a File')],
                            className="upload-text"
                        ),
                        multiple=True  # Allow multiple file uploads
                    )
                ],
                id='upload-container'  # Container for instruction and upload
            ),
            justify="center"
        ),
        # Loading spinner for file upload status
        dbc.Row(
            dcc.Loading(
                id="loading-spinner",
                type="circle",
                color="#3c9639",
                children=[
                    html.Div(id='output-upload'),  # This will be updated with the upload status
                    html.Div(id='analyse-button-placeholder')
                ]
            )
        )
    ],
    fluid=True,
)

# Combined callback for file upload, hiding upload button, and resetting the page
@app.callback(
    [
        Output('output-upload', 'children'),
        Output('upload-container', 'style'),  # Hide the upload box and instruction
        Output('analyse-button-placeholder', 'children')  # Show the Analyse button
    ],
    [
        Input('upload', 'contents'),
        Input('title', 'n_clicks')  # Detect click on the title/logo for reset
    ],
    [
        State('upload', 'filename')
    ]
)
def handle_file_upload_or_reset(contents, n_clicks, filename):
    # Check which input triggered the callback
    ctx = dash.callback_context
    if not ctx.triggered:
        return "No file uploaded yet.", {}, None

    # If the logo/title is clicked (reset case)
    if ctx.triggered[0]['prop_id'] == 'title.n_clicks':
        return "No file uploaded yet.", {}, None

    # If files are uploaded (file handling case)
    if contents is not None:
        ttl_files = []
        pkl_files = []
        
        for content, name in zip(contents, filename):
            content_type, content_string = content.split(',')
            decoded = base64.b64decode(content_string)

            # Check if it's a zip file
            if name.endswith('.zip'):
                with zipfile.ZipFile(io.BytesIO(decoded)) as zip_file:
                    # Extract file names within the zip
                    for file_name in zip_file.namelist():
                        if file_name.endswith('.ttl'):
                            ttl_files.append(file_name)
                        elif file_name.endswith('.pkl'):
                            pkl_files.append(file_name)

            else:
                # If not a zip, check the individual files
                if name.endswith('.ttl'):
                    ttl_files.append(name)
                elif name.endswith('.pkl'):
                    pkl_files.append(name)

        # Validation logic: ensure exactly one .ttl file and at least one .pkl file
        if len(ttl_files) == 1 and len(pkl_files) >= 1:
            analyse_button = dbc.Button("Analyse", id='analyse', color="success", outline="True")
            return f"Successfully uploaded: {ttl_files[0]} and {len(pkl_files)} .pkl file(s).", {"display": "none"}, analyse_button
        elif len(ttl_files) != 1:
            return "Error: Please upload exactly one .ttl file.", {}, None
        elif len(pkl_files) < 1:
            return "Error: Please upload at least one .pkl file.", {}, None
    
    return "No file uploaded yet.", {}, None

# Run the app
if __name__ == "__main__":
    app.run_server(debug=True, port=8050)

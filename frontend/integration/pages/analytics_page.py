from dash import html
from components.layout_components import create_title_logo
import os
import sys
from urllib.parse import parse_qs

# Add the root directory to the system path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../")))

# PipelineProgress class and pipeline functions defined
from backend.pipeline import get_pipeline_progress, data_pipeline

def layout(search):
    query_params = parse_qs(search.lstrip('?'))
    session_id = query_params.get('session_id', [None])[0]

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

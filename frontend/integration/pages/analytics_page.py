from dash import html
import dash_bootstrap_components as dbc
from components.layout_components import create_sidebar
from components.modals import create_warning_modal
import os
import sys

# Add the root directory to the system path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../")))

# PipelineProgress class and pipeline functions defined
from backend.pipeline import get_pipeline_progress

# Layout of analytics page
def analytics_layout():
    # Retrieve progress information (adjusted to no longer rely on session_id)
    progress = get_pipeline_progress()
    
    if progress:
        analytics = progress.get_analytics()
        if analytics:
            modified_file_names = analytics.get('file_names', [])
            dataset_name = analytics.get('dataset_name', '')
            uploaded_files = analytics.get('uploaded_files', [])
            
            # Content for the "Main" tab when progress is available
            main_tab_content = html.Div([
                html.H3(f"Dataset Name: {dataset_name}"),
                html.H4("Uploaded Files:"),
                html.Ul([html.Li(name) for name in uploaded_files]),
                html.H4("Modified Filenames:"),
                html.Ul([html.Li(name) for name in modified_file_names])
            ])
        else:
            # Fallback if progress exists but analytics are missing
            main_tab_content = html.Div([
                html.H4("No detailed analytics available.")
            ])
    else:
        # Content for the "Main" tab when no progress is available
        main_tab_content = html.Div([
            html.H4("No analytics available.")
        ])
    
    # Create the layout with a sidebar and the main content area using dbc.Row and dbc.Col
    return dbc.Container([
        dbc.Row([
            dbc.Col(create_sidebar(), width=2),  # Sidebar with fixed width
            dbc.Col([
                html.Br(),
                dbc.Tabs([
                    dbc.Tab(main_tab_content, label="Main", tab_id="main")
                ], id="analytics-tabs", active_tab="main"),
                create_warning_modal()
            ], width=10)  # Main content takes the rest of the space
        ])
    ], fluid=True)

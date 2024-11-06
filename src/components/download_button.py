"""
TODO: Add module description
"""

from dash import dcc, html
import dash_bootstrap_components as dbc


def create_global_download_button() -> html.Div:
    """
    Create a global download button along with its Download component and feedback message.

    Returns:
        html.Div: A Div containing the download button, Download component, and feedback message,
                  styled to be fixed at the bottom right.
    """
    download_button = dbc.Button(
        "Download All Dataframes as CSVs",
        id="global-download-button",
        n_clicks=0,
        className="download-button",
        outline=True,
        color="success",
        size="lg",
    )

    download_component = dcc.Download(id="global-download-data")

    download_feedback = html.Div(id="download-feedback")

    return html.Div(
        [
            download_button,
            download_component,
            download_feedback,
        ],
        id="global-download-container",
        className="d-grid gap-2 d-md-flex justify-content-md-end",
    )

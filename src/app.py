#!/usr/bin/env python3
"""
Main application module for the Dash web app.

This module initialises and configures the Dash application. It sets up the app layout,
registers callbacks, and handles command-line arguments for running the app in test mode
or with real data files. It uses the provided plot configurations to create the app's
visualisations and interactive components.
"""

import argparse
import sys

from dash import Dash
import dash_bootstrap_components as dbc

from analytics.analyticsmgr import AnalyticsManager
from analytics.dbmgr import DBManager
from analytics.dbmgr import DBManagerFileNotFoundError
from analytics.dbmgr import DBManagerBadCsvFile
from analytics.dbmgr import DBManagerBadRdfFile
from analytics.dbmgr import DBManagerBadZipFile
from callbacks.download_button_callbacks import register_download_callbacks
from callbacks.general_callbacks import register_general_callbacks
from callbacks.plot_callbacks import register_plot_callbacks
from components.layout import create_layout
from helpers.helpers import create_category_structure
from models.types import PlotConfig
from sampledata.plot_configs import sample_plot_configs

# Name of the application
APP_NAME = "Green InSight"


def main(arg_list: list[str] | None = None) -> None:
    """
    Main function for running the 'Green InSight' Building Time Series
    Visualisation application.

    Parses command-line arguments, initialises the dashboard, and runs
    the app.
    """
    args = parse_args(arg_list)

    if args.test_mode:
        plot_configs = sample_plot_configs
    else:
        try:
            db = DBManager(
                args.data, args.mapper, args.model, args.schema, args.building
            )
        except (
            DBManagerFileNotFoundError,
            DBManagerBadCsvFile,
            DBManagerBadRdfFile,
            DBManagerBadZipFile,
        ) as e:
            sys.exit(f"Error: {e}")

        am = AnalyticsManager(db)
        plot_configs = am.run_analytics()

    create_app(plot_configs).run(debug=args.debug, host=args.host, port=args.port)


# pylint: disable=unused-argument
def parse_args(arg_list: list[str] | None):
    """
    Parse command-line arguments.

    Args:
        arg_list (list[str] | None): List of command-line arguments to parse.
            If None, uses sys.argv.

    Returns:
        argparse.Namespace: Parsed command-line arguments.
    """

    # Parse command-line arguments
    parser = argparse.ArgumentParser(
        description="Building Time Series Visualisation",
    )
    # By enabling debug mode, the server will automatically reload if code changes,
    # and will show an interactive debugger in the browser if an error occurs
    # during a request.
    parser.add_argument(
        "-d",
        "--debug",
        help="Enable Flask debug mode (default: %(default)s)",
        action="store_true",
    )
    parser.add_argument(
        "-a",
        "--host",
        help="Host address used to serve the application (default: %(default)s)",
        default="127.0.0.1",
    )
    parser.add_argument(
        "-p",
        "--port",
        help="Port used to serve the application (default: %(default)s)",
        default=8050,
        type=int,
    )
    parser.add_argument(
        "-b",
        "--building",
        help="Filter mapper and data based on building (default: %(default)s)",
        default=None,
    )

    # Optional test mode argument, will load sample data and visualisations if enabled
    parser.add_argument(
        "-t",
        "--test-mode",
        help="Run the app using built-in sample data (default: %(default)s)",
        action="store_true",
    )

    # Positional arguments for file paths, not required if test mode is enabled
    parser.add_argument("data", help="Path to the timeseries zip file", nargs="?")
    parser.add_argument("mapper", help="Path to the mapper csv file", nargs="?")
    parser.add_argument("model", help="Path to the model ttl file", nargs="?")
    parser.add_argument(
        "schema",
        help="Path to the schema ttl file (default: %(default)s, load latest Brick schema)",
        nargs="?",
        default=None,  # Will load the latest Brick schema if not provided
    )

    args = parser.parse_args()

    # Custom validation logic for mutual exclusivity
    if args.test_mode:
        # If test mode is enabled, ensure no file paths are provided
        if any([args.data, args.mapper, args.model, args.schema]):
            parser.error("Cannot specify file paths when --test-mode is enabled.")
    else:
        # If test mode is not enabled, ensure all required file paths are provided
        required_files = [args.data, args.mapper, args.model]
        if any(arg is None for arg in required_files):
            parser.error(
                "Data, mapper, and model file paths are required if --test-mode is not enabled."
            )

    return args


def create_app(plot_configs: PlotConfig) -> Dash:
    """
    Initialize and configure the Dash application.

    Returns:
        Dash: Configured Dash application instance.
    """
    # Initialize the Dash app with Bootstrap styling
    app = Dash(
        __name__,
        external_stylesheets=[dbc.themes.BOOTSTRAP],
        suppress_callback_exceptions=True,
    )
    app.title = APP_NAME

    # Create category structure for tabs and retrieve mappings
    categories, category_key_mapping, subcategory_key_mapping = (
        create_category_structure(plot_configs.keys())
    )
    categories_structure = (categories, category_key_mapping, subcategory_key_mapping)

    # Set the app layout
    app.layout = create_layout(categories_structure)

    # Register download, general and plot-related callbacks
    register_download_callbacks(app, plot_configs)
    register_general_callbacks(app, plot_configs, categories_structure)
    register_plot_callbacks(app, plot_configs)

    return app


if __name__ == "__main__":
    main()

from dash import Dash
import dash_bootstrap_components as dbc

from analytics.analyticsmgr import AnalyticsManager
from analytics.dbmgr import DBManager
from callbacks.download_button_callbacks import register_download_callbacks
from callbacks.general_callbacks import register_general_callbacks
from callbacks.plot_callbacks import register_plot_callbacks
from components.layout import create_layout
from helpers.helpers import create_category_structure
from sampledata.plot_configs import sample_plot_configs

# Name of the application
APP_NAME = "Network in Progress"


def create_app(analytics_manager) -> Dash:
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

    # @tim: FIXME: Remove sample_plot_configs before submission?
    plot_configs = sample_plot_configs
    plot_configs |= analytics_manager.run_analytics()

    # Create category structure for tabs and retrieve mappings
    categories, category_key_mapping, subcategory_key_mapping = (
        create_category_structure(plot_configs.keys())
    )
    categories_structure = (categories, category_key_mapping, subcategory_key_mapping)

    # Set the app layout
    app.layout = create_layout(plot_configs, categories_structure)

    # Register download, general and plot-related callbacks
    register_download_callbacks(app, plot_configs)
    register_general_callbacks(app, plot_configs, categories_structure)
    register_plot_callbacks(app, plot_configs)

    return app


if __name__ == "__main__":
    # # Parse command-line arguments
    # parser = argparse.ArgumentParser()
    # parser.add_argument("data", help="Path to the data file")
    # parser.add_argument("mapper", help="Path to the mapper file")
    # parser.add_argument("model", help="Path to the model file")
    # parser.add_argument(
    #     "schema", help="Path to the schema file", nargs="?", default=None
    # )
    # parser.add_argument("-d", "--debug", help="Enable debug mode", action="store_true")
    # args = parser.parse_args()

    # # Load the data
    # db = dbmgr.DBManager(args.data, args.mapper, args.model, args.schema)

    # Hard-coded version for convenience during development
    DATA = "../datasets/bts_site_b_train/train.zip"
    MAPPER = "../datasets/bts_site_b_train/mapper_TrainOnly.csv"
    MODEL = "../datasets/bts_site_b_train/Site_B.ttl"
    SCHEMA = "../datasets/bts_site_b_train/Brick_v1.2.1.ttl"
    DEBUG = True

    # Load the data
    db = DBManager(DATA, MAPPER, MODEL, SCHEMA)

    # Load the analytics manager
    am = AnalyticsManager(db)

    # Run the Dash app server with debug mode enabled
    create_app(am).run_server(debug=DEBUG)

"""
This module is responsible for managing the analytics modules.  It imports all
modules from the `modules` sub-directory and runs them, collecting the plot
configurations returned by each module, to be used in the dashboard.
"""

import importlib
import os
import sys

from tqdm import tqdm

from models.types import PlotConfig


class AnalyticsManager:
    """
    Class for managing the analytics modules.

    Args:
        db (DBManager): An instance of the DBManager class.

    Returns:
        AnalyticsManager: An instance of the AnalyticsManager class.
    """

    def __init__(self, db) -> None:
        """Initialise the AnalyticsManager.

        Args:
            db (DBManager): An instance of the DBManager class.
        """
        self._db = db
        self._modules = []

        # Get the path to the `modules` sub-directory
        modules_dir = os.path.join(os.path.dirname(__file__), "modules")

        # Add the `modules` sub-directory to sys.path so it can be imported
        sys.path.append(modules_dir)

        # Iterate through files in the `modules` sub-directory
        for filename in sorted(os.listdir(modules_dir)):
            # Check for .py files, excluding __init__.py
            if filename.endswith(".py") and filename != "__init__.py":
                module_name = filename[:-3]  # Strip the .py extension

                try:
                    # Dynamically import the module
                    module = importlib.import_module(module_name)
                    self._modules.append(module)
                except ImportError as e:
                    print(f"Failed to import {module_name}: {e}", file=sys.stderr)

    def run_analytics(self) -> PlotConfig:
        """Run the analytics modules and collect the plot configurations.

        Returns:
            PlotConfig: A dictionary containing the plot configurations.
        """
        plot_configs = {}

        for module in tqdm(self._modules, desc="Running analytics modules"):
            try:
                module_results = module.run(self._db)
                plot_configs |= module_results
            except Exception as err:
                print(f"Unexpected {err=}, {type(err)=}", file=sys.stderr)
                raise

        return plot_configs

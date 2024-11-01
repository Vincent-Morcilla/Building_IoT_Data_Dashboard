"""
This module is responsible for managing the analytics modules.  It imports all
modules from the 'analytics' directory and runs them, collecting the plot
configurations returned by each module.
"""

import importlib
import os
import sys

from tqdm import tqdm


class AnalyticsManager:
    """
    Class for managing the analytics modules.

    Args:
        db (DBManager): An instance of the DBManager class.

    Returns:
        AnalyticsManager: An instance of the AnalyticsManager class.
    """

    def __init__(self, db):
        """Initialize the AnalyticsManager.

        Args:
            db (DBManager): An instance of the DBManager class.
        """
        self._db = db
        self._modules = []

        # Get the path to the 'analytics' directory
        analytics_dir = os.path.join(os.path.dirname(__file__), "modules")

        # Add the analytics directory to sys.path so it can be imported
        sys.path.append(analytics_dir)

        # Iterate through files in the 'analytics' directory
        for filename in sorted(os.listdir(analytics_dir)):
            # Check for .py files, excluding __init__.py
            # if filename.endswith(".py") and filename != "__init__.py":
            if filename.endswith(".py") and not filename.startswith("_"):
                module_name = filename[:-3]  # Strip the .py extension

                try:
                    # Dynamically import the module
                    module = importlib.import_module(module_name)
                    self._modules.append(module)
                    # print(f"Successfully imported {module_name}")
                except Exception as e:
                    print(f"Failed to import {module_name}: {e}")

    def run_analytics(self):
        """Run the analytics modules."""
        plot_configs = {}

        for module in tqdm(self._modules):
            try:
                module_results = module.run(self._db)
                plot_configs |= module_results
            except Exception:
                continue

        return plot_configs

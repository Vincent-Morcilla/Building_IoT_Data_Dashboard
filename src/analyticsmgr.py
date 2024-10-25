# import warnings

# warnings.filterwarnings("ignore")


import importlib
import os
import sys


class AnalyticsManager:
    def __init__(self, db):
        self._db = db
        self._modules = []

        # Get the path to the 'analytics' directory
        analytics_dir = os.path.join(os.path.dirname(__file__), "analytics")

        # Add the analytics directory to sys.path so it can be imported
        sys.path.append(analytics_dir)

        # Iterate through files in the 'analytics' directory
        for filename in os.listdir(analytics_dir):
            # Check for .py files, excluding __init__.py
            if filename.endswith(".py") and filename != "__init__.py":
                module_name = filename[:-3]  # Strip the .py extension

                try:
                    # Dynamically import the module
                    module = importlib.import_module(module_name)
                    self._modules.append(module)
                    # print(f"Successfully imported {module_name}")
                except Exception as e:
                    print(f"Failed to import {module_name}: {e}")

    def run_analytics(self):
        plot_configs = {}
        for module in self._modules:
            try:
                instance = module.Analytics(self._db)
                # instance = module.Analytics()
                plot_configs |= instance.run()
            except Exception as e:
                print(f"Error running analytics: {e}")
                continue

        return plot_configs

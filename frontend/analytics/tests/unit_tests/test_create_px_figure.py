import pytest
import pandas as pd
from plotly.graph_objects import Figure
from components.plot_generator import create_px_figure


def test_create_px_figure():
    """
    Test that the create_px_figure function returns a Plotly Figure instance.

    The function should take a function name, a DataFrame, and a set of keyword arguments
    for the plotly express function, and return a valid Figure object.
    """
    # Test data
    df = pd.DataFrame(
        {"Category": ["A", "B", "C", "A", "B", "C"], "Value": [10, 15, 7, 12, 18, 5]}
    )

    # Define function name and kwargs
    func_name = "bar"
    kwargs = {"x": "Category", "y": "Value", "color": "Category", "data_frame": df}

    # Call the function
    fig = create_px_figure(func_name, df, kwargs)

    # Assert the output is a plotly Figure instance
    assert isinstance(fig, Figure)

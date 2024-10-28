import pytest
import pandas as pd
import plotly.graph_objects as go

from components.plot_generator import create_go_figure

# Sample data for testing
df = pd.DataFrame({
    'Category': ['A', 'B', 'C', 'A', 'B', 'C'],
    'Value': [10, 15, 7, 12, 18, 5]
})

# Configuration for the plot component
component = {
    'trace_type': 'Bar',
    'data_processing': {
        'data_mappings': {
            'x': 'Category',
            'y': 'Value',
        },
        'trace_kwargs': {
            'name': 'Value by Category',
        }
    }
}

kwargs = {}  # Additional kwargs for go.Figure()

# Generate the figure
fig_go = create_go_figure(df, component['data_processing'], component, kwargs)


def test_create_go_figure():
    """Test that the create_go_figure function returns a Plotly Figure instance."""
    assert isinstance(fig_go, go.Figure)

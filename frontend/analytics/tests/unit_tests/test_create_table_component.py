from dash import dash_table, html
import pandas as pd
from components.plot_generator import create_table_component

# Sample DataFrame to test the create_table_component function.
df_table = pd.DataFrame({
    'Name': ['Alice', 'Bob', 'Charlie'],
    'Age': [25, 30, 35],
    'City': ['Sydney', 'Melbourne', 'Queensland']
})

# Define a sample component dictionary to pass to create_table_component.
component = {
    'id': 'test-table',
    'type': 'table',
    'dataframe': df_table,
    'kwargs': {
        'columns': [
            {'name': 'Name', 'id': 'Name'},
            {'name': 'Age', 'id': 'Age'},
            {'name': 'City', 'id': 'City'}
        ],
        'page_size': 10
    }
}

# Generate the table component using the function.
table_component = create_table_component(component)


def test_create_table_component():
    """
    Test to ensure create_table_component returns a valid Dash component.

    The function should return a DataTable instance or an html.Div containing 
    a DataTable. This checks the main component type for the table generated.
    """
    assert isinstance(table_component, dash_table.DataTable) or (
        isinstance(table_component, html.Div) and 
        isinstance(table_component.children, dash_table.DataTable)
    )

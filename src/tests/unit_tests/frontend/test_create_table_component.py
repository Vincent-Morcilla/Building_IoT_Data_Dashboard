from dash import dash_table, html
import pandas as pd
from components.plot_generator import create_table_component

# Sample DataFrame to test the create_table_component function.
df_table = pd.DataFrame(
    {
        "Name": ["Alice", "Bob", "Charlie"],
        "Age": [25, 30, 35],
        "City": ["Sydney", "Melbourne", "Queensland"],
    }
)

# Define a sample component dictionary to pass to create_table_component.
component = {
    "id": "test-table",
    "type": "table",
    "dataframe": df_table,
    "kwargs": {
        "columns": [
            {"name": "Name", "id": "Name"},
            {"name": "Age", "id": "Age"},
            {"name": "City", "id": "City"},
        ],
        "page_size": 10,
    },
}

# Generate the table component using the function.
table_component = create_table_component(component)


def test_create_table_component():
    """
    Test to ensure create_table_component returns a valid Dash component.

    The function should return an html.Div containing the DataTable,
    possibly along with a title. This test checks that the component
    is an html.Div and contains a dash_table.DataTable among its children.
    """
    # Check that the component is an html.Div
    assert isinstance(table_component, html.Div), "The component should be an html.Div"

    # Check that the Div has children
    assert hasattr(table_component, "children"), "The Div should have children"

    # Get the list of children
    children = table_component.children

    # Ensure children is a list or tuple
    if not isinstance(children, (list, tuple)):
        children = [children]

    # Check that one of the children is a DataTable
    has_datatable = any(isinstance(child, dash_table.DataTable) for child in children)

    assert has_datatable, "No DataTable found in the component's children"

    # Optionally, check that the DataTable has the expected data and columns
    for child in children:
        if isinstance(child, dash_table.DataTable):
            # Check data
            assert child.data == df_table.to_dict(
                "records"
            ), "Data in DataTable does not match expected data"
            # Check columns
            assert (
                child.columns == component["kwargs"]["columns"]
            ), "Columns in DataTable do not match expected columns"

    # Since no title is specified, we expect only the DataTable
    assert (
        len(children) == 1
    ), "Component should only contain the DataTable since no title is specified"


component_with_title = {
    "id": "test-table-with-title",
    "type": "table",
    "dataframe": df_table,
    "title": "Sample Table Title",
    "title_element": "H2",
    "title_kwargs": {
        "style": {
            "textAlign": "center",
            "color": "#333",
        }
    },
    "kwargs": {
        "columns": [
            {"name": "Name", "id": "Name"},
            {"name": "Age", "id": "Age"},
            {"name": "City", "id": "City"},
        ],
        "page_size": 10,
    },
}

# Generate the table component with title
table_component_with_title = create_table_component(component_with_title)


def test_create_table_component_with_title():
    """
    Test to ensure create_table_component returns a valid Dash component with a title.
    """
    # Check that the component is an html.Div
    assert isinstance(
        table_component_with_title, html.Div
    ), "The component should be an html.Div"

    # Check that the Div has children
    assert hasattr(
        table_component_with_title, "children"
    ), "The Div should have children"

    # Get the list of children
    children = table_component_with_title.children

    # Ensure children is a list or tuple
    if not isinstance(children, (list, tuple)):
        children = [children]

    # Expecting two children: the title and the DataTable
    assert len(children) == 2, "Component should contain a title and a DataTable"

    # Check that the first child is the title
    title_component = children[0]
    assert isinstance(
        title_component, html.H2
    ), "The first child should be an html.H2 element"
    assert (
        title_component.children == "Sample Table Title"
    ), "The title text does not match"

    # Check that the second child is a DataTable
    table = children[1]
    assert isinstance(
        table, dash_table.DataTable
    ), "The second child should be a DataTable"

    # Optionally, check data and columns as before
    assert table.data == df_table.to_dict(
        "records"
    ), "Data in DataTable does not match expected data"
    assert (
        table.columns == component_with_title["kwargs"]["columns"]
    ), "Columns in DataTable do not match expected columns"

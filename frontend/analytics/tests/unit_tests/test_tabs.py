import pytest
from dash import html
import dash_bootstrap_components as dbc
from components.tabs import create_tab_content

# Test the create_tab_content function with dynamically generated inputs.
@pytest.mark.parametrize(
    "category_name, subcategory, expected_label, expected_id",
    [
        ("Category1", "Subcategory1", "Subcategory1", "category1-subcategory1"),
        ("AnotherCategory", "AnotherSubcategory", "AnotherSubcategory", "anothercategory-anothersubcategory"),
        ("DataScience", "MachineLearning", "MachineLearning", "datascience-machinelearning"),
    ]
)
def test_create_dynamic_tab_content(category_name, subcategory, expected_label, expected_id):
    tab_content = create_tab_content(category_name, subcategory)

    # Assert that the returned component is a dbc.Tab
    assert isinstance(tab_content, dbc.Tab)

    # Check that the tab has the correct label and ID
    assert tab_content.label == expected_label
    assert tab_content.tab_id == expected_id

    # Check the container contents (header and paragraph)
    container = tab_content.children
    assert isinstance(container, dbc.Container)

    h4, p = container.children
    assert isinstance(h4, html.H4)
    assert h4.children == f"{subcategory} Content"

    assert isinstance(p, html.P)
    assert p.children == f"This is placeholder content for {subcategory} under {category_name}."

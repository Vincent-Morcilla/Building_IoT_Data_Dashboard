import pytest
from dash import html
import dash_bootstrap_components as dbc
from unittest.mock import patch
from components.tabs import create_tab_content, create_tab_layout
from helpers.helpers import create_category_structure

@pytest.fixture
def mock_plot_configs():
    """
    Fixture that returns a minimal structure mimicking the essential structure of plot_configs.

    Returns:
        dict: A dictionary structure with mock data for plot configuration.
    """
    return {
        ("DataQuality", "ConsumptionDataQuality"): {
            "title": "Consumption Data Quality Analysis",
            "components": [
                {
                    "type": "plot",
                    "library": "px",
                    "function": "box",
                    "id": "consumption-quality-box-plot",
                    "kwargs": {
                        "data_frame": {
                            "Measurement": ["Quality A", "Quality B"],
                            "Value": [60, 75],
                        },
                        "x": "Measurement",
                        "y": "Value",
                        "color": "Measurement",
                    },
                },
            ]
        },
        ("Consumption", "GeneralAnalysis"): {
            "title": "General Analysis",
            "components": [
                {
                    "type": "plot",
                    "library": "px",
                    "function": "line",
                    "id": "consumption-line-plot",
                    "kwargs": {
                        "data_frame": {
                            "Timestamp": ["2023-01-01", "2023-01-02"],
                            "Usage": [500, 520],
                        },
                        "x": "Timestamp",
                        "y": "Usage",
                    },
                },
            ]
        },
    }

@pytest.fixture
def category_mappings(mock_plot_configs):
    """
    Generate category mappings using the `create_category_structure` helper function.

    Args:
        mock_plot_configs (dict): The plot configuration structure for testing.

    Returns:
        tuple: Categories, category_key_mapping, and subcategory_key_mapping.
    """
    analysis_keys = list(mock_plot_configs.keys())
    categories, category_key_mapping, subcategory_key_mapping = create_category_structure(analysis_keys)
    return categories, category_key_mapping, subcategory_key_mapping

@pytest.mark.parametrize(
    "category_name, subcategory, expected_label, expected_id, expected_content",
    [
        ("Data Quality", "Consumption Data Quality", "Consumption Data Quality", "data-quality-consumption-data-quality", "Consumption Data Quality Analysis"),
        ("Data Quality", "NonExistentSubcategory", "NonExistentSubcategory", "data-quality-nonexistentsubcategory", "No content available for NonExistentSubcategory under Data Quality."),
        ("Unknown Category", "Unknown Subcategory", "Unknown Subcategory", "unknown-category-unknown-subcategory", "No content available for Unknown Subcategory under Unknown Category. Please select a different option."),
    ]
)
def test_create_tab_content(category_name, subcategory, expected_label, expected_id, expected_content, mock_plot_configs, category_mappings):
    """
    Test the `create_tab_content` function for various category and subcategory inputs.

    Args:
        category_name (str): Name of the category.
        subcategory (str): Name of the subcategory.
        expected_label (str): Expected label for the tab.
        expected_id (str): Expected tab ID.
        expected_content (str): Expected content in the tab.
        mock_plot_configs (dict): Mocked plot configurations for testing.
        category_mappings (tuple): Mappings for categories and subcategories.
    """
    categories, category_key_mapping, subcategory_key_mapping = category_mappings

    with patch('components.tabs.plot_configs', mock_plot_configs):
        with patch('components.tabs.create_layout_for_category', return_value=[html.Div([html.H4("Mocked Content for Tab")])]):
            tab_content = create_tab_content(category_name, subcategory, category_key_mapping, subcategory_key_mapping)

            if category_name not in category_key_mapping:
                # When the category is unknown, expect an html.Div
                assert isinstance(tab_content, html.Div)
                h4_element = tab_content.children[0]
                assert isinstance(h4_element, html.H4)
                assert h4_element.children == expected_content
            else:
                # When the category exists, expect a dbc.Tab
                assert isinstance(tab_content, dbc.Tab)
                assert tab_content.label == expected_label
                assert tab_content.tab_id == expected_id

                container = tab_content.children
                assert isinstance(container, dbc.Container)

                if "No content available" in expected_content:
                    # The content is an html.Div within the container
                    content_div = container.children
                    if isinstance(content_div, list):
                        content_div = content_div[0]
                    assert isinstance(content_div, html.Div)
                    h4_element = content_div.children
                    if isinstance(h4_element, list):
                        h4_element = h4_element[0]
                    assert isinstance(h4_element, html.H4)
                    assert h4_element.children == expected_content
                else:
                    # Content from create_layout_for_category
                    content_list = container.children
                    if isinstance(content_list, list):
                        content_div = content_list[0]
                    else:
                        content_div = content_list
                    h4_element = content_div.children[0]
                    assert isinstance(h4_element, html.H4)
                    # Check for mocked content
                    assert "Mocked Content for Tab" in h4_element.children

@pytest.mark.parametrize(
    "selected_category, expected_tab_ids",
    [
        ("Data Quality", ["data-quality-consumption-data-quality"]),
        ("Consumption", ["consumption-general-analysis"]),
        ("Unknown Category", [])
    ]
)
def test_create_tab_layout(selected_category, expected_tab_ids, mock_plot_configs, category_mappings):
    """
    Test the `create_tab_layout` function for different categories to check if the correct tab layout is generated.

    Args:
        selected_category (str): Category name selected.
        expected_tab_ids (list): Expected list of tab IDs for the category.
        mock_plot_configs (dict): Mocked plot configurations for testing.
        category_mappings (tuple): Mappings for categories and subcategories.
    """
    categories, category_key_mapping, subcategory_key_mapping = category_mappings

    with patch('components.tabs.plot_configs', mock_plot_configs):
        with patch('components.tabs.create_layout_for_category', return_value=[html.Div([html.H4("Mocked Content for Tab")])]):
            tab_layout = create_tab_layout(selected_category, categories, category_key_mapping, subcategory_key_mapping)

            if not expected_tab_ids:
                assert isinstance(tab_layout, html.Div)
                assert len(tab_layout.children) == 1
                h4 = tab_layout.children[0]  # Access the html.H4 directly
                assert isinstance(h4, html.H4)
                assert "No content available" in h4.children
            else:
                assert isinstance(tab_layout, html.Div)
                tabs_component = tab_layout.children[0]
                assert isinstance(tabs_component, dbc.Tabs)

                actual_tab_ids = [tab.tab_id for tab in tabs_component.children]
                assert actual_tab_ids == expected_tab_ids

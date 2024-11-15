"""
Handles the creation of tabbed content within the application.

This module generates the content for individual tabs based on the selected
category and subcategory. It constructs the tab layout by creating tabs for each
subcategory under a selected category, enabling organized and accessible
navigation through the app's visualizations and components.
"""

from dash import html
import dash_bootstrap_components as dbc

from components.download_button import create_global_download_button
from components.analytics import create_layout_for_category
from models.types import (
    PlotConfig,
    Categories,
    CategoryKeyMapping,
    SubcategoryKeyMapping,
)


def create_tab_content(
    plot_configs: PlotConfig,
    category_name: str,
    subcategory: str,
    category_key_mapping: CategoryKeyMapping,
    subcategory_key_mapping: SubcategoryKeyMapping,
) -> dbc.Tab:
    """
    Create the content for an individual tab.

    Args:
        plot_configs (PlotConfig): Dictionary containing plot configuration.
        category_name (str): The display name of the category.
        subcategory (str): The display name of the subcategory.
        category_key_mapping (CategoryKeyMapping): A dictionary mapping display category names
                                                   to their original keys.
        subcategory_key_mapping (SubcategoryKeyMapping): A dictionary mapping
                                                         (category, subcategory)
                                                         to original subcategory keys.

    Returns:
        dbc.Tab: A Dash Tab component with the content for the given category and subcategory.
    """
    # Get the original category and subcategory keys
    category_key = category_key_mapping.get(category_name)
    sub_cat_key = subcategory_key_mapping.get((category_name, subcategory))

    # Handle cases where category or subcategory does not exist
    if category_key is None:
        return html.Div(
            [
                html.H4(
                    f"No content available for {subcategory} under {category_name}. Please select an option from the sidebar."
                )
            ]
        )

    if sub_cat_key is None:
        sub_cat_key = "Main"
    key = (category_key, sub_cat_key)

    # Check if the key exists in plot_configs and generate content
    if key in plot_configs:
        config = plot_configs[key]
        content = create_layout_for_category(config)
    else:
        content = html.Div(
            [
                html.H4(
                    f"No content available for {subcategory} under {category_name}. Please select an option from the sidebar."
                )
            ]
        )

    # Return the tab component
    return dbc.Tab(
        dbc.Container(content, className="tab-content"),
        label=subcategory,
        tab_id=f"{category_name.lower().replace(' ', '-').replace('_', '-')}-{subcategory.lower().replace(' ', '-').replace('_', '-')}",
    )


def create_tab_layout(
    plot_configs: PlotConfig,
    selected_category: str,
    categories: Categories,
    category_key_mapping: CategoryKeyMapping,
    subcategory_key_mapping: SubcategoryKeyMapping,
) -> html.Div:
    """
    Create a layout containing tabs for the selected category.

    Args:
        plot_configs (PlotConfig): Dictionary containing plot configuration.
        selected_category (str): The category selected from the URL or user input.
        categories (Categories): A dictionary where keys are display category names,
                                 and values are lists of subcategories.
        category_key_mapping (CategoryKeyMapping): A dictionary mapping display category names
                                                   to their original keys.
        subcategory_key_mapping (SubcategoryKeyMapping): A dictionary mapping
                                                         (category, subcategory)
                                                         to original subcategory keys.

    Returns:
        html.Div: A Dash Div component containing the layout of tabs for the selected category,
        a horizontal rule, and a global download button.
    """
    # Convert and standardize category name from the URL
    selected_category_name = selected_category.replace("-", " ")

    # Match the display category name
    matched_category = None
    for cat_display_name in categories.keys():
        if cat_display_name.lower() == selected_category_name.lower():
            matched_category = cat_display_name
            break

    # Handle case where no matching category is found
    if matched_category is None:
        return html.Div(
            [
                html.H4(
                    f"No content available for {selected_category_name}. Please select an option from the sidebar."
                )
            ]
        )

    # Get subcategories for the matched category
    subcategories = categories[matched_category]

    # Create tabs for each subcategory
    tabs = [
        create_tab_content(
            plot_configs,
            matched_category,
            subcat,
            category_key_mapping,
            subcategory_key_mapping,
        )
        for subcat in subcategories
    ]

    # Create the Tabs component for the selected category
    tabs_component = dbc.Tabs(
        id=f"tabs-{matched_category.lower().replace(' ', '-').replace('_', '-')}",
        active_tab=(
            tabs[0].tab_id if tabs else None
        ),  # Default to the first tab if available
        children=tabs,
    )

    # Create the global download button
    global_download_button = create_global_download_button()

    # Return the tab layout wrapped in a Div
    return html.Div(
        [
            tabs_component,
            html.Hr(),
            global_download_button,
        ]
    )

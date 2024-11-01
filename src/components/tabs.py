from dash import html
import dash_bootstrap_components as dbc
from components.plot_generator import create_layout_for_category


def create_tab_content(
    plot_configs,
    category_name,
    subcategory,
    category_key_mapping,
    subcategory_key_mapping,
):
    """
    Create the content for an individual tab.

    Args:
        category_name (str): The display name of the category.
        subcategory (str): The display name of the subcategory.
        category_key_mapping (dict): A dictionary mapping display category names to their original keys.
        subcategory_key_mapping (dict): A dictionary mapping (category, subcategory) to original subcategory keys.

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
                    f"No content available for {subcategory} under {category_name}. Please select a different option."
                )
            ]
        )

    if sub_cat_key is None:
        sub_cat_key = "Main"
    key = (category_key, sub_cat_key)

    # Check if the key exists in plot_configs and generate content
    if key in plot_configs:
        config = plot_configs[key]
        content = create_layout_for_category(key, config)
    else:
        content = html.Div(
            [html.H4(f"No content available for {subcategory} under {category_name}.")]
        )

    # Return the tab component
    return dbc.Tab(
        dbc.Container(content, className="tab-content"),
        label=subcategory,
        tab_id=f"{category_name.lower().replace(' ', '-').replace('_', '-')}-{subcategory.lower().replace(' ', '-').replace('_', '-')}",
    )


def create_tab_layout(
    plot_configs,
    selected_category,
    categories,
    category_key_mapping,
    subcategory_key_mapping,
):
    """
    Create a layout containing tabs for the selected category.

    Args:
        selected_category (str): The category selected from the URL or user input.
        categories (dict): A dictionary where the keys are display category names, and the values are lists of subcategories.
        category_key_mapping (dict): A dictionary mapping display category names to their original keys.
        subcategory_key_mapping (dict): A dictionary mapping (category, subcategory) to original subcategory keys.

    Returns:
        html.Div: A Dash Div component containing the layout of tabs for the selected category.
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
            [html.H4(f"No content available for {selected_category_name}.")]
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

    # Return the tab layout wrapped in a Div
    return html.Div([tabs_component])

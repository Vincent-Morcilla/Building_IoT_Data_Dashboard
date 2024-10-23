
from dash import html
import dash_bootstrap_components as dbc

# Creates tab contents
def create_tab_content(category_name, subcategory):
    # Return a tab with placeholder content
    return dbc.Tab(
        dbc.Container(
            [
                html.H4(f"{subcategory} Content"),
                html.P(f"This is placeholder content for {subcategory} under {category_name}."),
            ]
        ),
        label=subcategory,  # Label for the tab
        tab_id=f"{category_name.lower().replace(' ', '-')}-{subcategory.lower().replace(' ', '-')}",
    )

# Creates a layout for tabs based on the selected category
def create_tab_layout(selected_category, categories):
    # Convert category name from the URL (replace '-' with ' ') and capitalize appropriately
    selected_category_name = selected_category.replace('-', ' ').title()

    # Standardize the category keys to be lowercase
    categories = {k.lower(): v for k, v in categories.items()}
    selected_category_name = selected_category_name.lower()

    # Retrieve subcategories for the selected category
    subcategories = categories.get(selected_category_name, ["Main"])  # Defaults to 'Main' if no subcategories
    tabs = []

    for subcat in subcategories:
        tab_content = create_tab_content(selected_category_name, subcat)
        tabs.append(tab_content)

    # Create the Tabs component for the selected category
    tabs_component = dbc.Tabs(
        id=f"tabs-{selected_category_name.replace(' ', '-')}",
        active_tab=tabs[0].tab_id,  # Default to the first tab
        children=tabs
    )

    # Return the tab layout for the selected category
    return html.Div([tabs_component])

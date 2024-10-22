import re
from collections import defaultdict

# Converts a string written in PascalCase into a space-separated string for better readability.
def pascal_to_words(text):
    return re.sub(r"(?<!^)(?=[A-Z])", " ", text)

# Creates a dictionary structure for categories and subcategories from a list of analysis tuples.
def create_category_structure(analysis_list):
    categories = {}
    for key in analysis_list:
        # Check if key is a tuple, if not, treat it as a single category with no subcategory
        if isinstance(key, tuple):
            main_cat, sub_cat = key
        else:
            main_cat, sub_cat = key, None

        main_cat = pascal_to_words(main_cat)

        # Handle cases where sub_cat is None or empty
        if not sub_cat:
            sub_cat = "Main"
        else:
            sub_cat = pascal_to_words(sub_cat)

        if main_cat not in categories:
            categories[main_cat] = []
        if sub_cat and sub_cat not in categories[main_cat]:
            categories[main_cat].append(sub_cat)

    return categories

# Generates a URL mapping from a dictionary of plot configurations.
def create_url_mapping(plot_configs):
    url_mapping = defaultdict(list)
    
    for key in plot_configs.keys():
        # Check if key is a tuple, if not, treat it as a single category with no subcategory
        if isinstance(key, tuple):
            main_cat, sub_cat = key
        else:
            main_cat, sub_cat = key, None

        # Convert the main category to a lowercase, URL-friendly format
        url_friendly_key = main_cat.lower()

        # Handle cases where sub_cat is None or empty
        if sub_cat:
            # If a subcategory exists, create a URL in the format "/maincategory/subcategory"
            url_mapping[main_cat.lower()].append(f"/{main_cat.lower()}/{sub_cat.lower()}")
        else:
            # If no subcategory, create a URL in the format "/maincategory"
            url_mapping[main_cat.lower()].append(f"/{main_cat.lower()}")

    return url_mapping


"""
Helper functions for string manipulation and category structure creation.

This module contains utility functions that assist with string formatting,
such as converting PascalCase to space-separated words and sanitizing filenames.
It also includes a function to create the category and subcategory structure
used in the app's sidebar navigation based on the plot configurations.
"""

import re
from typing import Tuple

from models.types import PlotConfigsKeys
from models.types import Categories
from models.types import CategoryKeyMapping
from models.types import SubcategoryKeyMapping


def pascal_to_words(text: str) -> str:
    """
    Convert a PascalCase string into a space-separated string.

    Args:
        text (str): The input string in PascalCase.

    Returns:
        str: A space-separated version of the input string.
    """
    return " ".join(re.findall(r"[A-Za-z0-9][^A-Z]*", text))


def sanitise_filename(text: str) -> str:
    """
    Sanitise a string to be used as a filename by replacing invalid characters with underscores.

    Args:
        text (str): The input string.

    Returns:
        str: A sanitised string safe for use as a filename.
    """
    return re.sub(r"[^A-Za-z0-9_\-]", "_", text)


def create_category_structure(
    plot_configs_keys: PlotConfigsKeys,
) -> Tuple[
    Categories,  # Dict[str, List[str]]
    CategoryKeyMapping,  # Dict[str, str]
    SubcategoryKeyMapping,  # Dict[Tuple[str, str], str]
]:
    """
    Create a dictionary structure for categories and subcategories from plot_configs keys.

    Args:
        plot_configs_keys (List[Union[Tuple[str, str], str]]): A list of keys from plot_configs,
                              where each key is a tuple representing a category and optionally
                              a subcategory.

    Returns:
        Tuple: A tuple containing:
            - categories (Dict[str, List[str]]): A dictionary where keys are display names
                                                 for categories, and values are lists
                                                 of subcategories.
            - category_key_mapping (Dict[str, str]): A dictionary mapping display names
                                                     to the original category keys.
            - subcategory_key_mapping (Dict[Tuple[str, str], str]): A dictionary mapping
                                             (display category, display subcategory)
                                             to the original subcategory keys.
    """
    categories = {}
    category_key_mapping = {}  # Map from display name to original key
    subcategory_key_mapping = (
        {}
    )  # Map from (display name, display subcategory) to original subcategory

    for key in plot_configs_keys:
        if isinstance(key, tuple):
            main_cat, sub_cat = key
        else:
            main_cat, sub_cat = key, None

        display_main_cat = pascal_to_words(main_cat)
        display_sub_cat = "Main" if not sub_cat else pascal_to_words(sub_cat)

        if display_main_cat not in categories:
            categories[display_main_cat] = []
            category_key_mapping[display_main_cat] = main_cat
        if display_sub_cat not in categories[display_main_cat]:
            categories[display_main_cat].append(display_sub_cat)
            subcategory_key_mapping[(display_main_cat, display_sub_cat)] = sub_cat

    return categories, category_key_mapping, subcategory_key_mapping

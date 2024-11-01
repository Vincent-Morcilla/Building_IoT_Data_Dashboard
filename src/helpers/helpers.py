import re


def pascal_to_words(text):
    """
    Convert a PascalCase string into a space-separated string.

    Args:
        text (str): The input string in PascalCase.

    Returns:
        str: A space-separated version of the input string.
    """
    return " ".join(re.findall(r"[A-Za-z][^A-Z]*", text))


def sanitise_filename(text):
    """
    Sanitise a string to be used as a filename by replacing invalid characters with underscores.

    Args:
        text (str): The input string.

    Returns:
        str: A sanitised string safe for use as a filename.
    """
    return re.sub(r"[^A-Za-z0-9_\-]", "_", text)


def create_category_structure(analysis_list):
    """
    Create a dictionary structure for categories and subcategories from a list of analysis tuples.

    Args:
        analysis_list (list): A list of tuples or strings, where each tuple contains a
                              category and optionally a subcategory.

    Returns:
        tuple: A tuple containing:
            - categories (dict): A dictionary where keys are display names for categories,
                                 and values are lists of subcategories.
            - category_key_mapping (dict): A dictionary mapping display names to the original
                                           category keys.
            - subcategory_key_mapping (dict): A dictionary mapping (display category, display
                                              subcategory) to the original subcategory keys.
    """
    categories = {}
    category_key_mapping = {}  # Map from display name to original key
    subcategory_key_mapping = (
        {}
    )  # Map from (display name, display subcategory) to original subcategory

    for key in analysis_list:
        if isinstance(key, tuple):
            main_cat, sub_cat = key
        else:
            main_cat, sub_cat = key, None

        display_main_cat = pascal_to_words(main_cat)

        if not sub_cat:
            display_sub_cat = "Main"
        else:
            display_sub_cat = pascal_to_words(sub_cat)

        if display_main_cat not in categories:
            categories[display_main_cat] = []
            category_key_mapping[display_main_cat] = main_cat
        if display_sub_cat and display_sub_cat not in categories[display_main_cat]:
            categories[display_main_cat].append(display_sub_cat)
            subcategory_key_mapping[(display_main_cat, display_sub_cat)] = sub_cat

    return categories, category_key_mapping, subcategory_key_mapping

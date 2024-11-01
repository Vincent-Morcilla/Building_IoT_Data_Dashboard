import pytest
from helpers.helpers import create_category_structure


def test_create_category_structure():
    """
    Test the create_category_structure function with various inputs.

    This function tests the following scenarios:
    - Category and subcategory provided as a tuple.
    - Category provided without a subcategory.
    - Single category provided (not as a tuple).
    - Empty input list.
    """

    # Test case: Category with a subcategory provided as a tuple
    analysis_list = [("MainCategory", "SubCategory")]
    categories, category_key_mapping, subcategory_key_mapping = (
        create_category_structure(analysis_list)
    )
    assert categories["Main Category"] == [
        "Sub Category"
    ], "Subcategory mapping failed for tuple input"
    assert (
        category_key_mapping["Main Category"] == "MainCategory"
    ), "Category key mapping failed"
    assert (
        subcategory_key_mapping[("Main Category", "Sub Category")] == "SubCategory"
    ), "Subcategory key mapping failed"

    # Test case: Category provided without a subcategory
    analysis_list = [("MainCategory", None)]
    categories, category_key_mapping, subcategory_key_mapping = (
        create_category_structure(analysis_list)
    )
    assert categories["Main Category"] == [
        "Main"
    ], "Default subcategory handling failed"
    assert (
        category_key_mapping["Main Category"] == "MainCategory"
    ), "Category key mapping failed"
    assert (
        subcategory_key_mapping[("Main Category", "Main")] is None
    ), "Subcategory key mapping for None failed"

    # Test case: Single category provided (not as a tuple)
    analysis_list = ["SingleCategory"]
    categories, category_key_mapping, subcategory_key_mapping = (
        create_category_structure(analysis_list)
    )
    assert categories["Single Category"] == ["Main"], "Single category handling failed"
    assert (
        category_key_mapping["Single Category"] == "SingleCategory"
    ), "Category key mapping failed"
    assert (
        subcategory_key_mapping[("Single Category", "Main")] is None
    ), "Subcategory key mapping for single category failed"

    # Test case: Empty input
    analysis_list = []
    categories, category_key_mapping, subcategory_key_mapping = (
        create_category_structure(analysis_list)
    )
    assert categories == {}, "Empty input handling failed"
    assert category_key_mapping == {}, "Empty category key mapping failed"
    assert subcategory_key_mapping == {}, "Empty subcategory key mapping failed"

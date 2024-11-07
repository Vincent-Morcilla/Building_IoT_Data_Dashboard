import pytest
from helpers.helpers import create_category_structure


def test_category_with_subcategory():
    """Test handling of a category with a subcategory."""
    analysis_list = [("MainCategory", "SubCategory")]
    categories, category_key_mapping, subcategory_key_mapping = (
        create_category_structure(analysis_list)
    )

    assert categories == {
        "Main Category": ["Sub Category"]
    }, "Failed to map category with subcategory correctly"
    assert category_key_mapping == {
        "Main Category": "MainCategory"
    }, "Failed to map category key correctly"
    assert subcategory_key_mapping == {
        ("Main Category", "Sub Category"): "SubCategory"
    }, "Failed to map subcategory key correctly"


def test_category_without_subcategory():
    """Test handling of a category without a subcategory."""
    analysis_list = [("MainCategory", None)]
    categories, category_key_mapping, subcategory_key_mapping = (
        create_category_structure(analysis_list)
    )

    assert categories == {
        "Main Category": ["Main"]
    }, "Failed to handle category without subcategory correctly"
    assert category_key_mapping == {
        "Main Category": "MainCategory"
    }, "Failed to map category key correctly"
    assert subcategory_key_mapping == {
        ("Main Category", "Main"): None
    }, "Failed to handle None subcategory correctly"


def test_single_category():
    """Test handling of a single category without a tuple."""
    analysis_list = ["SingleCategory"]
    categories, category_key_mapping, subcategory_key_mapping = (
        create_category_structure(analysis_list)
    )

    assert categories == {
        "Single Category": ["Main"]
    }, "Failed to handle single category correctly"
    assert category_key_mapping == {
        "Single Category": "SingleCategory"
    }, "Failed to map single category key correctly"
    assert subcategory_key_mapping == {
        ("Single Category", "Main"): None
    }, "Failed to handle single category subcategory correctly"


def test_empty_input():
    """Test handling of empty input."""
    analysis_list = []
    categories, category_key_mapping, subcategory_key_mapping = (
        create_category_structure(analysis_list)
    )

    assert categories == {}, "Failed to handle empty input correctly"
    assert (
        category_key_mapping == {}
    ), "Failed to handle empty category mapping correctly"
    assert (
        subcategory_key_mapping == {}
    ), "Failed to handle empty subcategory mapping correctly"


@pytest.mark.parametrize(
    "analysis_list, expected_categories, expected_category_mapping, expected_subcategory_mapping",
    [
        (
            [("MainCategory", "SubCategory")],
            {"Main Category": ["Sub Category"]},
            {"Main Category": "MainCategory"},
            {("Main Category", "Sub Category"): "SubCategory"},
        ),
        (
            [("MainCategory", None)],
            {"Main Category": ["Main"]},
            {"Main Category": "MainCategory"},
            {("Main Category", "Main"): None},
        ),
        (
            ["SingleCategory"],
            {"Single Category": ["Main"]},
            {"Single Category": "SingleCategory"},
            {("Single Category", "Main"): None},
        ),
        ([], {}, {}, {}),
        # Edge case: empty string category and subcategory
        (
            [("", "")],
            {"": ["Main"]},
            {"": ""},
            {("", "Main"): ""},
        ),
        # Edge case: duplicate category with different subcategories
        (
            [("Category", "SubA"), ("Category", "SubB")],
            {"Category": ["Sub A", "Sub B"]},
            {"Category": "Category"},
            {("Category", "Sub A"): "SubA", ("Category", "Sub B"): "SubB"},
        ),
        # Edge case: duplicate subcategories within the same category
        (
            [("Category", "Sub"), ("Category", "Sub")],
            {"Category": ["Sub"]},
            {"Category": "Category"},
            {("Category", "Sub"): "Sub"},
        ),
    ],
)
def test_create_category_structure_parametrized(
    analysis_list,
    expected_categories,
    expected_category_mapping,
    expected_subcategory_mapping,
):
    """
    Parametrized test for create_category_structure with multiple cases.

    Args:
        analysis_list (list): Input list with categories and optional subcategories.
        expected_categories (dict): Expected output dictionary of categories.
        expected_category_mapping (dict): Expected mapping of display names to category keys.
        expected_subcategory_mapping (dict): Expected mapping of (display category, display subcategory)
                                             to original subcategory keys.
    """
    categories, category_key_mapping, subcategory_key_mapping = (
        create_category_structure(analysis_list)
    )

    assert (
        categories == expected_categories
    ), f"Categories mismatch for input {analysis_list}"
    assert (
        category_key_mapping == expected_category_mapping
    ), f"Category key mapping mismatch for input {analysis_list}"
    assert (
        subcategory_key_mapping == expected_subcategory_mapping
    ), f"Subcategory key mapping mismatch for input {analysis_list}"

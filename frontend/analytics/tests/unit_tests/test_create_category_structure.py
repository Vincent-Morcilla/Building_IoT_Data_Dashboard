import pytest
from helpers.helpers import create_category_structure

def test_create_category_structure():
    # Test with category and subcategory as a tuple
    analysis_list = [("MainCategory", "SubCategory")]
    categories = create_category_structure(analysis_list)
    assert categories["Main Category"] == ["Sub Category"]

    # Test with category but no subcategory
    analysis_list = [("MainCategory", None)]
    categories = create_category_structure(analysis_list)
    assert categories["Main Category"] == ["General"]

    # Test with a single category (no tuple)
    analysis_list = ["SingleCategory"]
    categories = create_category_structure(analysis_list)
    assert categories["Single Category"] == ["General"]

    # Test empty input
    analysis_list = []
    categories = create_category_structure(analysis_list)
    assert categories == {}

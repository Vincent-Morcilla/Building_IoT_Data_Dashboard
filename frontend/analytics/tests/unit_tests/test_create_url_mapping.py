import pytest
from helpers.helpers import create_url_mapping

def test_create_url_mapping():
    # Test with both category and subcategory
    plot_configs = {("CategoryOne", "SubCategory"): {}, "CategoryTwo": {}}
    url_mapping = create_url_mapping(plot_configs)
    assert url_mapping["categoryone"] == ["/categoryone/subcategory"]
    assert url_mapping["categorytwo"] == ["/categorytwo"]

    # Test with single category (no subcategory)
    plot_configs = {"CategoryOnly": {}}
    url_mapping = create_url_mapping(plot_configs)
    assert url_mapping["categoryonly"] == ["/categoryonly"]

    # Test empty input
    plot_configs = {}
    url_mapping = create_url_mapping(plot_configs)
    assert url_mapping == {}

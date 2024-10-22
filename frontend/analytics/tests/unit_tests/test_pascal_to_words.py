import pytest
from helpers.helpers import pascal_to_words

def test_pascal_to_words():
    # Test standard PascalCase string
    assert pascal_to_words("PascalCaseText") == "Pascal Case Text"

    # Test single-word input
    assert pascal_to_words("Word") == "Word"

    # Test empty string input
    assert pascal_to_words("") == ""

    # Test input with no PascalCase
    assert pascal_to_words("nocamelcase") == "nocamelcase"

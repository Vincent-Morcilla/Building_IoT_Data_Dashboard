import pytest
from helpers.helpers import pascal_to_words

def test_pascal_to_words():
    """Test the pascal_to_words function for various cases."""
    
    # Test standard PascalCase string
    assert pascal_to_words("PascalCaseText") == "Pascal Case Text", "Failed on standard PascalCase input"

    # Test single-word input
    assert pascal_to_words("Word") == "Word", "Failed on single-word input"

    # Test empty string input
    assert pascal_to_words("") == "", "Failed on empty string input"

    # Test input with no PascalCase
    assert pascal_to_words("nocamelcase") == "nocamelcase", "Failed on input without PascalCase"

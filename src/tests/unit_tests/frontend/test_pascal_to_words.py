from helpers.helpers import pascal_to_words


def test_pascal_to_words_standard_case():
    """
    Test pascal_to_words with a standard PascalCase input.
    Ensures the function correctly splits words with uppercase letters.
    """
    result = pascal_to_words("PascalCaseText")
    assert result == "Pascal Case Text", "Failed on standard PascalCase input"


def test_pascal_to_words_single_word():
    """
    Test pascal_to_words with a single word input.
    Ensures the function returns the word itself without modification.
    """
    result = pascal_to_words("Word")
    assert result == "Word", "Failed on single-word input"


def test_pascal_to_words_empty_string():
    """
    Test pascal_to_words with an empty string.
    Ensures the function returns an empty string.
    """
    result = pascal_to_words("")
    assert result == "", "Failed on empty string input"


def test_pascal_to_words_no_pascal_case():
    """
    Test pascal_to_words with a lowercase, non-PascalCase string.
    Ensures the function returns the input without modification.
    """
    result = pascal_to_words("nocamelcase")
    assert result == "nocamelcase", "Failed on input without PascalCase"

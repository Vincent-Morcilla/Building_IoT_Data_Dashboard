from helpers.helpers import sanitise_filename


def test_sanitise_filename_allows_valid_characters():
    """
    Test that the function leaves valid characters (A-Z, a-z, 0-9, -, _) intact.
    """
    assert sanitise_filename("Valid_File-Name123") == "Valid_File-Name123"


def test_sanitise_filename_replaces_spaces():
    """
    Test that the function replaces spaces with underscores.
    """
    assert sanitise_filename("Invalid File Name") == "Invalid_File_Name"


def test_sanitise_filename_replaces_special_characters():
    """
    Test that the function replaces special characters with underscores.
    """
    assert sanitise_filename("Invalid!File@Name#") == "Invalid_File_Name_"


def test_sanitise_filename_handles_empty_string():
    """
    Test that the function returns an empty string when given an empty input.
    """
    assert sanitise_filename("") == ""


def test_sanitise_filename_mixed_invalid_characters():
    """
    Test that the function replaces a mix of invalid characters correctly.
    """
    assert (
        sanitise_filename("File*Name:With<>Invalid/Chars")
        == "File_Name_With__Invalid_Chars"
    )


def test_sanitise_filename_unicode_characters():
    """
    Test that the function replaces unicode characters with underscores.
    """
    assert sanitise_filename("file_😊_name") == "file___name"

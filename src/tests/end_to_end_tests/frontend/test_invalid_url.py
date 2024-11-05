import sys
import warnings

import pytest
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager
from webdriver_manager.core.os_manager import ChromeType


@pytest.fixture(scope="module")
def driver():
    """Fixture to initialize and yield a Selenium WebDriver instance for testing."""
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", category=DeprecationWarning)

        chrome_options = Options()

        # Run headless if necessary
        if sys.platform == "linux":
            chrome_options.add_argument("--headless")
            chrome_options.add_argument("--window-size=1920,1080")
            chrome_options.add_argument("--disable-gpu")
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            service = Service(
                ChromeDriverManager(chrome_type=ChromeType.CHROMIUM).install()
            )
        else:  # Windows and macOS
            service = Service(ChromeDriverManager().install())

        driver = webdriver.Chrome(service=service, options=chrome_options)

    yield driver
    driver.quit()


def test_invalid_url(driver):
    """Test to verify that navigating to an invalid URL shows the appropriate error message."""
    BASE_URL = "http://localhost:8050"
    INVALID_PATH = "/invalid-url"

    # Navigate to the invalid URL
    driver.get(BASE_URL + INVALID_PATH)

    wait = WebDriverWait(driver, 20)  # Increase wait time if necessary

    # Adjust XPath based on actual HTML structure
    error_message_xpath = "//div[@id='page-content']//h4"

    try:
        # Wait for the error message element to be present
        error_element = wait.until(
            EC.presence_of_element_located((By.XPATH, error_message_xpath))
        )
        error_text = error_element.text.strip()

        # Use the exact error message from the application
        expected_text = "No content available for invalid url. Please select an option from the sidebar."

        assert (
            error_text == expected_text
        ), f"Expected error message to be '{expected_text}', but got '{error_text}'."

        print("Test Passed: Appropriate error message is displayed for invalid URL.")

    except AssertionError as e:
        # Print the actual error text for debugging
        print(f"Test Failed. Actual error message: '{error_text}'")
        pytest.fail(f"Test Failed: {e}")

    except Exception as e:
        # Print the page source for debugging
        print("Test Failed. Page source at the time of failure:")
        print(driver.page_source)
        pytest.fail(f"Test Failed: {e}")

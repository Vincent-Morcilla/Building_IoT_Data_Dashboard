import multiprocessing
import os
import sys
import time
import warnings

import pytest
import requests
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from app import create_app
from sampledata.plot_configs import sample_plot_configs

# Define the host and port
HOST = "127.0.0.1"
PORT = 61523
BASE_URL = f"http://{HOST}:{PORT}"


def run_app():
    """Create and run the Dash application."""
    app = create_app(sample_plot_configs)
    app.run(debug=False, host=HOST, port=PORT)


@pytest.fixture(scope="module")
def app_process():
    """Run the Dash app in a separate process.

    Yields:
        multiprocessing.Process: The process running the app.
    """
    proc = multiprocessing.Process(target=run_app)
    proc.start()
    # Wait for the server to start
    start_time = time.time()
    timeout = 30  # seconds
    while True:
        try:
            requests.get(BASE_URL)
            break
        except requests.exceptions.ConnectionError:
            time.sleep(1)
            if time.time() - start_time > timeout:
                proc.terminate()
                raise Exception("Failed to start the app within the timeout period.")
    yield proc
    proc.terminate()
    proc.join()


@pytest.fixture(scope="module")
def driver(app_process):
    """Initialize and yield a Selenium WebDriver instance for testing.

    Args:
        app_process (multiprocessing.Process): The process running the app.

    Yields:
        selenium.webdriver.Chrome: The Chrome WebDriver instance.
    """
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", category=DeprecationWarning)

    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")

    service = Service(ChromeDriverManager().install())
    driver_instance = webdriver.Chrome(service=service, options=chrome_options)

    yield driver_instance
    driver_instance.quit()


def test_invalid_url(driver):
    """Verify that navigating to an invalid URL shows the appropriate error message.

    Args:
        driver (selenium.webdriver.Chrome): The Selenium WebDriver instance.
    """
    invalid_path = "/invalid-url"

    # Navigate to the invalid URL
    driver.get(BASE_URL + invalid_path)

    wait = WebDriverWait(driver, 20)

    # Adjust XPath based on actual HTML structure
    error_message_xpath = "//div[@id='page-content']//h4"

    try:
        # Wait for the error message element to be present
        error_element = wait.until(
            EC.presence_of_element_located((By.XPATH, error_message_xpath))
        )
        error_text = error_element.text.strip()

        # Use the exact error message from the application
        expected_text = (
            "No content available for invalid url. "
            "Please select an option from the sidebar."
        )

        assert error_text == expected_text, (
            f"Expected error message to be '{expected_text}', "
            f"but got '{error_text}'."
        )

        print("Test Passed: Appropriate error message is displayed for invalid URL.")

    except AssertionError as assertion_error:
        # Print the actual error text for debugging
        print(f"Test Failed. Actual error message: '{error_text}'")
        pytest.fail(f"Test Failed: {assertion_error}")

    except Exception as general_exception:
        # Print the page source for debugging
        print("Test Failed. Page source at the time of failure:")
        print(driver.page_source)
        pytest.fail(f"Test Failed: {general_exception}")

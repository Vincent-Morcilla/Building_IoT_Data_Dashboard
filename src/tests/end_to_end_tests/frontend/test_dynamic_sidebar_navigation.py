import multiprocessing
import os
import sys
import time
import warnings

import pytest
from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from app import create_app

# Define the host and port
HOST = "127.0.0.1"
PORT = 8051
BASE_URL = f"http://{HOST}:{PORT}"

# Define a minimal sample_plot_configs within the test file
sample_plot_configs = {
    ("Category1", "Subcategory1"): {
        "title": "Sample Title 1",
        "components": [],
    },
    ("Category2", "Subcategory2"): {
        "title": "Sample Title 2",
        "components": [],
    },
}


def run_app():
    """Create and run the Dash application."""
    app = create_app(sample_plot_configs)
    app.run_server(debug=False, host=HOST, port=PORT)


@pytest.fixture(scope="module")
def driver():
    """Initialize the Selenium WebDriver for Chrome.

    Yields:
        selenium.webdriver.Chrome: The Chrome WebDriver instance.
    """
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", category=DeprecationWarning)

    chrome_options = Options()
    chrome_options.add_argument("--headless")  # Run in headless mode
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")

    service = Service(ChromeDriverManager().install())
    driver_instance = webdriver.Chrome(service=service, options=chrome_options)

    yield driver_instance
    driver_instance.quit()


@pytest.fixture(scope="module")
def app_process():
    """Run the Dash app in a separate process.

    Yields:
        multiprocessing.Process: The process running the app.
    """
    proc = multiprocessing.Process(target=run_app)
    proc.start()
    # Wait for the server to start
    time.sleep(5)  # Wait a moment for the server to start

    yield proc

    # Terminate the process after the test
    proc.terminate()
    proc.join()


def test_sidebar_options(driver, app_process):
    """Test that non-Home sidebar options are working using mock sample configs.

    Args:
        driver (selenium.webdriver.Chrome): The Selenium WebDriver instance.
        app_process (multiprocessing.Process): The process running the app.
    """
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC

    wait = WebDriverWait(driver, 10)

    # Open the application
    driver.get(BASE_URL)

    # Wait for the sidebar to load
    wait.until(EC.presence_of_element_located((By.CLASS_NAME, "sidebar")))

    # Get the names of all non-Home sidebar links
    nav_links = driver.find_elements(By.CSS_SELECTOR, ".sidebar .nav-link")
    non_home_link_texts = [
        link.text.strip() for link in nav_links if link.text.strip() != "Home"
    ]

    assert non_home_link_texts, "No non-Home sidebar options found."

    # Iterate over each non-Home sidebar option
    for option_name in non_home_link_texts:
        # Navigate to the base URL and wait for the sidebar
        driver.get(BASE_URL)
        wait.until(EC.presence_of_element_located((By.CLASS_NAME, "sidebar")))

        # Re-find the link by its text
        try:
            link = wait.until(EC.element_to_be_clickable((By.LINK_TEXT, option_name)))
            link.click()
        except TimeoutException:
            raise Exception(f"Link with text '{option_name}' not found")

        # Wait for the content to load
        wait.until(EC.presence_of_element_located((By.ID, "page-content")))

        # Check for tabs (subcategories)
        tab_elements = driver.find_elements(By.CSS_SELECTOR, ".nav-tabs .nav-link")
        if tab_elements:
            # There are tabs, iterate over them
            tab_texts = [tab.text.strip() for tab in tab_elements]
            for tab_name in tab_texts:
                # Click the tab
                try:
                    tab = wait.until(
                        EC.element_to_be_clickable((By.LINK_TEXT, tab_name))
                    )
                    tab.click()
                except TimeoutException:
                    raise Exception(
                        f"Tab with text '{tab_name}' not found under '{option_name}'"
                    )

                # Wait for content to load
                wait.until(EC.presence_of_element_located((By.ID, "page-content")))

                try:
                    content_title_element = wait.until(
                        EC.presence_of_element_located((By.TAG_NAME, "h2"))
                    )
                    content_title = content_title_element.text.strip()
                except TimeoutException:
                    content_title = ""

                # Build the key to access the plot config
                category_key = option_name.replace(" ", "")
                subcategory_key = tab_name.replace(" ", "")
                config_key = (category_key, subcategory_key)

                expected_title = sample_plot_configs.get(config_key, {}).get(
                    "title", ""
                )
                assert (
                    content_title == expected_title
                ), f"Content title does not match for {option_name} - {tab_name}"
        else:
            # No tabs, proceed
            try:
                content_title_element = wait.until(
                    EC.presence_of_element_located((By.TAG_NAME, "h2"))
                )
                content_title = content_title_element.text.strip()
            except TimeoutException:
                content_title = ""

            # Build the key to access the plot config
            category_key = option_name.replace(" ", "")
            subcategory_key = category_key  # Assume subcategory is same as category
            config_key = (category_key, subcategory_key)

            expected_title = sample_plot_configs.get(config_key, {}).get("title", "")
            assert (
                content_title == expected_title
            ), f"Content title does not match for {option_name}"

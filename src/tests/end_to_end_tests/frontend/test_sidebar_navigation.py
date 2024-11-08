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
PORT = 8051
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
    """Initialize the Selenium WebDriver for Chrome.

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


def test_sidebar_navigation(driver):
    """Test navigation through the sidebar and logo button clicks.

    Args:
        driver (selenium.webdriver.Chrome): The Selenium WebDriver instance.
    """
    driver.get(BASE_URL)

    wait = WebDriverWait(driver, 10)

    # Verify the 'Home' link is clickable and click it
    home_link = wait.until(EC.element_to_be_clickable((By.LINK_TEXT, "Home")))
    try:
        home_link.click()
    except Exception:
        driver.execute_script("arguments[0].click();", home_link)

    # Assert the URL is the homepage
    wait.until(EC.url_to_be(f"{BASE_URL}/"))
    assert driver.current_url == f"{BASE_URL}/"

    # Verify the 'Logo' button is clickable and click it
    logo_button = wait.until(EC.element_to_be_clickable((By.ID, "logo-button")))
    try:
        logo_button.click()
    except Exception:
        driver.execute_script("arguments[0].click();", logo_button)

    # Assert the URL is the homepage after clicking the logo
    wait.until(EC.url_to_be(f"{BASE_URL}/"))
    assert driver.current_url == f"{BASE_URL}/"

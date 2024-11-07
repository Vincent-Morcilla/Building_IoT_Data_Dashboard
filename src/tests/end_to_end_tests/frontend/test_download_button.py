import multiprocessing
import os
import shutil
import sys
import time
import warnings

import pytest
import requests
from selenium import webdriver
from selenium.common.exceptions import (
    TimeoutException,
    ElementClickInterceptedException,
)
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

DOWNLOAD_DIR = os.path.join(
    os.getcwd(), "downloads"
)  # Directory to save downloaded files


def setup_download_dir(download_dir):
    """Create or clean the download directory.

    Args:
        download_dir (str): The path to the download directory.
    """
    if not os.path.exists(download_dir):
        os.makedirs(download_dir)
    else:
        # Clean up any existing files
        shutil.rmtree(download_dir)
        os.makedirs(download_dir)


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
    setup_download_dir(DOWNLOAD_DIR)
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", category=DeprecationWarning)

    chrome_options = Options()
    prefs = {
        "download.default_directory": DOWNLOAD_DIR,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
    }
    chrome_options.add_experimental_option("prefs", prefs)
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")

    service = Service(ChromeDriverManager().install())

    driver_instance = webdriver.Chrome(service=service, options=chrome_options)

    yield driver_instance
    driver_instance.quit()


def test_download_button(driver):
    """Test the download button functionality.

    Args:
        driver (selenium.webdriver.Chrome): The Selenium WebDriver instance.
    """
    wait = WebDriverWait(driver, 30)

    try:
        # Open the Dash application
        driver.get(BASE_URL)

        # Wait until the sidebar options have loaded
        wait.until(EC.presence_of_element_located((By.CLASS_NAME, "sidebar")))

        # Find all sidebar options (navigation links)
        nav_links = driver.find_elements(By.CSS_SELECTOR, ".sidebar .nav-link")

        # Exclude "Home" and click on another option
        for link in nav_links:
            if link.text.strip() != "Home":
                print(f"Clicking on sidebar option: {link.text.strip()}")
                link.click()
                break
        else:
            print("No sidebar option found other than 'Home'.")
            assert False, "No sidebar option found other than 'Home'."

        # Wait until the content area is present
        wait.until(EC.presence_of_element_located((By.ID, "page-content")))

        # Wait for any tabs to be present
        try:
            wait.until(EC.presence_of_element_located((By.CLASS_NAME, "nav-tabs")))
        except TimeoutException:
            pass  # Proceed if tabs are not present

        # Wait for the download button to be clickable
        download_button = wait.until(
            EC.element_to_be_clickable((By.ID, "global-download-button"))
        )

        # Scroll to the download button
        driver.execute_script("arguments[0].scrollIntoView(true);", download_button)
        time.sleep(1)  # Wait for scrolling to complete

        print("Clicking on the download button.")

        # Click the download button
        try:
            download_button.click()
        except ElementClickInterceptedException:
            print(
                "ElementClickInterceptedException caught. Trying to click via JavaScript."
            )
            driver.execute_script("arguments[0].click();", download_button)

        # Wait for the download to complete
        file_downloaded = False
        download_timeout = time.time() + 30  # 30 seconds from now

        while True:
            # Check if any zip files exist in the download directory
            files = [f for f in os.listdir(DOWNLOAD_DIR) if f.endswith(".zip")]
            if files:
                print(f"Download completed. File(s): {files}")
                file_downloaded = True
                break
            if time.time() > download_timeout:
                print("Download did not complete within the timeout period.")
                break
            time.sleep(1)

        assert file_downloaded, "Download did not complete within the timeout period."
        print("Test Passed: Download completed successfully.")

        # Clean up downloaded files
        for file_name in files:
            os.remove(os.path.join(DOWNLOAD_DIR, file_name))

    except Exception as error:
        print(f"An error occurred: {error}")
        raise

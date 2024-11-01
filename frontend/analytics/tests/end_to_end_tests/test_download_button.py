import os
import shutil
import sys
import time
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

DOWNLOAD_DIR = os.path.join(
    os.getcwd(), "downloads"
)  # Directory to save downloaded files


def setup_download_dir(download_dir):
    """Create or clean the download directory.

    If the directory does not exist, it is created.
    If it exists, it is removed along with its contents, and then recreated.

    Args:
        download_dir (str): The path to the download directory.
    """
    if not os.path.exists(download_dir):
        os.makedirs(download_dir)
    else:
        # Clean up any existing files
        shutil.rmtree(download_dir)
        os.makedirs(download_dir)


@pytest.fixture(scope="module")
def driver():
    """Initialize the Selenium WebDriver for Chrome.

    This fixture sets up the Chrome WebDriver with the required configuration,
    including download directory preferences and options. It yields a WebDriver
    instance for use in tests and quits the driver after the tests are done.

    Yields:
        WebDriver: Selenium WebDriver instance for Chrome.
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

        if sys.platform == "linux":
            chrome_options.add_argument("--headless")
            service = Service(
                ChromeDriverManager(chrome_type=ChromeType.CHROMIUM).install()
            )
        else:  # Windows and MacOS
            service = Service(ChromeDriverManager().install())

        driver = webdriver.Chrome(service=service, options=chrome_options)

    yield driver
    driver.quit()


def test_download_button(driver):
    """Test the download button functionality.

    This test performs the following steps:
    - Opens the Dash application.
    - Clicks on a sidebar option that is not 'Home'.
    - Clicks the 'Download All Dataframes as CSVs' button.
    - Checks that a zip file is downloaded.

    Args:
        driver (WebDriver): The Selenium WebDriver instance.
    """
    wait = WebDriverWait(driver, 30)

    try:
        # Open the Dash application
        driver.get("http://localhost:8050")

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
        time.sleep(10)  # Adjust sleep time if necessary

        # Wait for the download button to be clickable
        download_button = wait.until(
            EC.element_to_be_clickable((By.ID, "global-download-button"))
        )
        print("Clicking on the download button.")

        # Click the download button
        download_button.click()

        # Wait for the download to complete
        file_downloaded = False
        timeout = time.time() + 30  # 30 seconds from now

        while True:
            # Check if any zip files exist in the download directory
            files = [f for f in os.listdir(DOWNLOAD_DIR) if f.endswith(".zip")]
            if files:
                print(f"Download completed. File(s): {files}")
                file_downloaded = True
                break
            if time.time() > timeout:
                print("Download did not complete within the timeout period.")
                break
            time.sleep(1)

        assert (
            file_downloaded
        ), "Test Failed: Download did not complete within the timeout period."
        print("Test Passed: Download completed successfully.")

    except Exception as error:
        print(f"An error occurred: {error}")
        raise

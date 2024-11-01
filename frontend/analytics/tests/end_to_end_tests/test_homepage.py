import sys
import time
import warnings

import pytest
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
from webdriver_manager.core.os_manager import ChromeType


@pytest.fixture(scope="module")
def driver():
    """
    Fixture to initialize the Selenium WebDriver for Chrome with the required configuration.
    This suppresses DeprecationWarnings and sets up the ChromeDriver.
    Yields:
        WebDriver: Selenium WebDriver instance for Chrome.
    """
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", category=DeprecationWarning)

        chrome_options = Options()

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


def test_homepage_title(driver):
    """
    Test case to verify the homepage title and essential content on the page.
    This function:
        - Loads the homepage
        - Checks the page title
        - Confirms the presence of the title logo and relevant text
    Args:
        driver (WebDriver): The Selenium WebDriver instance from the fixture.
    """
    driver.get("http://localhost:8050")

    # Verify the page title
    assert driver.title == "Network in Progress"

    # Wait for page to fully load
    time.sleep(2)

    # Check if the homepage content (image and paragraph) is rendered
    title_logo = driver.find_element(By.CLASS_NAME, "title-logo")
    assert title_logo.get_attribute("alt") == "Title-Logo"

    text_element = driver.find_element(By.XPATH, "//p")
    assert text_element.text == "Select an option from the sidebar categories"

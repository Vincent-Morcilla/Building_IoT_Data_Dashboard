import pytest
import warnings
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service

@pytest.fixture(scope="module")
def driver():
    # Suppress DeprecationWarnings from urllib3 or Selenium
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", category=DeprecationWarning)
        # Initialize WebDriver with Service
        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))
    yield driver
    driver.quit()

def test_homepage_title(driver):
    # Load the homepage
    driver.get("http://localhost:8050")

    # Verify the page title matches what's set in app_config
    assert driver.title == "Network in Progress"
import pytest
import warnings
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
import time

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

    # Verify the page title
    assert driver.title == "Network in Progress"

    # Allow time for page to fully load
    time.sleep(2)

    # Check if the homepage content (image and paragraph) is rendered
    title_logo = driver.find_element(By.CLASS_NAME, "title-logo")
    assert title_logo.get_attribute("alt") == "Title-Logo"

    text_element = driver.find_element(By.XPATH, "//p")
    assert text_element.text == "Select an option from the sidebar categories"


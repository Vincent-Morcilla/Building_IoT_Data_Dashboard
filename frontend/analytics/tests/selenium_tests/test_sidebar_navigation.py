import pytest
import warnings
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

@pytest.fixture(scope="module")
def driver():
    # Suppress DeprecationWarning from urllib3 or Selenium
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", category=DeprecationWarning)
        # Service for WebDriver initialization
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service)
    yield driver
    driver.quit()

def test_sidebar_navigation(driver):
    # Load the homepage
    driver.get("http://localhost:8050")

    # Wait for the 'Home' link to appear and be clickable
    home_link = WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.LINK_TEXT, "Home"))
    )
    
    # Click on the 'Home' link
    try:
        home_link.click()
    except:
        # If it's still not interactable, force the click with JavaScript
        driver.execute_script("arguments[0].click();", home_link)

    # Verify that the URL changed back to the homepage
    assert driver.current_url == "http://localhost:8050/"

    # ---------------------------------
    # Test clicking on the logo button
    # ---------------------------------
    # Wait for the 'Logo' button to be clickable
    logo_button = WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.ID, "logo-button"))
    )
    
    # Click on the 'Logo' button
    try:
        logo_button.click()
    except:
        # If it's not interactable, use JavaScript to force the click
        driver.execute_script("arguments[0].click();", logo_button)

    # Verify that the URL changed back to the homepage after clicking the logo
    WebDriverWait(driver, 10).until(EC.url_to_be("http://localhost:8050/"))
    assert driver.current_url == "http://localhost:8050/"

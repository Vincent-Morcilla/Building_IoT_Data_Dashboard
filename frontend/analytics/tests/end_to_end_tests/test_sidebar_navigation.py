import warnings
import pytest
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from webdriver_manager.core.os_manager import ChromeType
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


@pytest.fixture(scope="module")
def driver():
    """Fixture to initialize and yield a Selenium WebDriver instance for testing."""
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", category=DeprecationWarning)
        # service = Service(ChromeDriverManager().install())
        # driver = webdriver.Chrome(service=service)
        chrome_options = Options()
        # chrome_options.add_argument("--headless")
        driver = webdriver.Chrome(
            service=Service(
                ChromeDriverManager(chrome_type=ChromeType.CHROMIUM).install()
            ),
            options=chrome_options,
        )
    yield driver
    driver.quit()


def test_sidebar_navigation(driver):
    """Test navigation through the sidebar and logo button clicks."""
    # Load the homepage
    driver.get("http://localhost:8050")

    # Verify the 'Home' link is clickable and click it
    home_link = WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.LINK_TEXT, "Home"))
    )
    try:
        home_link.click()
    except Exception:
        driver.execute_script("arguments[0].click();", home_link)

    # Assert the URL is the homepage
    assert driver.current_url == "http://localhost:8050/"

    # Verify the 'Logo' button is clickable and click it
    logo_button = WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.ID, "logo-button"))
    )
    try:
        logo_button.click()
    except Exception:
        driver.execute_script("arguments[0].click();", logo_button)

    # Assert the URL is the homepage after clicking the logo
    WebDriverWait(driver, 10).until(EC.url_to_be("http://localhost:8050/"))
    assert driver.current_url == "http://localhost:8050/"

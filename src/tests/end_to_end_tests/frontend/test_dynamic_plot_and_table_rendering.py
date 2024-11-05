import multiprocessing
import os
import sys
import time
import warnings

import pytest
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager

# Adjust the Python path if needed
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

# Import create_app from your application module
from app import create_app

# Define the host and port
HOST = "127.0.0.1"
PORT = 8050

# Define a minimal sample_plot_configs within the test file
sample_plot_configs = {
    ("Category1", "Subcategory1"): {
        "title": "Sample Title 1",
        "components": [
            {
                "type": "plot",
                "id": "plot1",
                "library": "plotly",
                "data": {
                    "x": [1, 2, 3],
                    "y": [4, 5, 6],
                },
                "layout": {
                    "title": "Sample Plot 1",
                },
            },
            {
                "type": "table",
                "id": "table1",
                "data": [
                    {"Column1": "Row1Col1", "Column2": "Row1Col2"},
                    {"Column1": "Row2Col1", "Column2": "Row2Col2"},
                ],
                "columns": ["Column1", "Column2"],
            },
        ],
    },
}


def run_app():
    # Create the app with the minimal sample configs
    app = create_app(sample_plot_configs)
    app.run_server(debug=False, host=HOST, port=PORT)


@pytest.fixture(scope="module")
def driver():
    """Initialize the Selenium WebDriver for Chrome."""
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", category=DeprecationWarning)

        chrome_options = Options()
        chrome_options.add_argument("--headless")  # Run in headless mode
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--window-size=1920,1080")

        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=chrome_options)

    yield driver
    driver.quit()


@pytest.fixture(scope="module")
def app_process():
    """Run the Dash app in a separate process."""
    # Start the app in a separate process
    proc = multiprocessing.Process(target=run_app)
    proc.start()
    time.sleep(5)  # Wait a moment for the server to start

    yield proc

    # Terminate the process after the test
    proc.terminate()
    proc.join()


def test_plot_and_table_rendering(driver, app_process):
    """Test that plots and tables are being rendered."""
    base_url = f"http://{HOST}:{PORT}"

    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC

    wait = WebDriverWait(driver, 10)

    # Open the application
    driver.get(base_url)

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
        driver.get(base_url)
        wait.until(EC.presence_of_element_located((By.CLASS_NAME, "sidebar")))

        # Re-find the link by its text
        try:
            link = wait.until(EC.element_to_be_clickable((By.LINK_TEXT, option_name)))
            link.click()
        except Exception as e:
            raise Exception(f"Link with text '{option_name}' not found: {str(e)}")

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
                except Exception as e:
                    raise Exception(
                        f"Tab with text '{tab_name}' not found under '{option_name}': {str(e)}"
                    )

                # Wait for content to load
                wait.until(EC.presence_of_element_located((By.ID, "page-content")))

                # Additional wait to ensure content is loaded
                time.sleep(1)

                # Verify the content title
                try:
                    content_title_element = wait.until(
                        EC.presence_of_element_located((By.TAG_NAME, "h2"))
                    )
                    content_title = content_title_element.text.strip()
                except:
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

                # Interact with elements within the tab
                components_expected = sample_plot_configs.get(config_key, {}).get(
                    "components", []
                )

                for component in components_expected:
                    if component["type"] == "plot":
                        # Verify the plot is rendered
                        try:
                            plot_element = wait.until(
                                EC.presence_of_element_located((By.ID, component["id"]))
                            )
                            assert (
                                plot_element.is_displayed()
                            ), f"Plot '{component['id']}' is not displayed in {option_name} - {tab_name}"
                            # Verify plot data
                        except Exception as e:
                            raise Exception(
                                f"Plot component with ID '{component['id']}' not found in {option_name} - {tab_name}: {str(e)}"
                            )
                    elif component["type"] == "table":
                        # Verify the table is rendered
                        try:
                            table_element = wait.until(
                                EC.presence_of_element_located((By.ID, component["id"]))
                            )
                            assert (
                                table_element.is_displayed()
                            ), f"Table '{component['id']}' is not displayed in {option_name} - {tab_name}"

                            # Extract table data from the DOM
                            table_rows = table_element.find_elements(
                                By.CSS_SELECTOR, 'div[role="row"]'
                            )
                            # Subtract one for header row
                            assert len(table_rows) - 1 == len(
                                component["data"]
                            ), f"Number of table rows does not match in {option_name} - {tab_name}"

                            # Verify table headers
                            header_cells = table_rows[0].find_elements(
                                By.CSS_SELECTOR, 'div[role="columnheader"]'
                            )
                            header_texts = [cell.text for cell in header_cells]
                            assert (
                                header_texts == component["columns"]
                            ), f"Table headers do not match in {option_name} - {tab_name}"

                            # Verify table data
                            for i, row_data in enumerate(component["data"], start=1):
                                row_cells = table_rows[i].find_elements(
                                    By.CSS_SELECTOR, 'div[role="gridcell"]'
                                )
                                cell_texts = [cell.text for cell in row_cells]
                                expected_texts = [
                                    str(row_data[col]) for col in component["columns"]
                                ]
                                assert (
                                    cell_texts == expected_texts
                                ), f"Row {i} data does not match in {option_name} - {tab_name}"
                        except Exception as e:
                            raise Exception(
                                f"Table component with ID '{component['id']}' not found or invalid in {option_name} - {tab_name}: {str(e)}"
                            )
        else:
            # No tabs
            try:
                content_title_element = wait.until(
                    EC.presence_of_element_located((By.TAG_NAME, "h2"))
                )
                content_title = content_title_element.text.strip()
            except:
                content_title = ""

            # Build the key to access the plot config
            category_key = option_name.replace(" ", "")
            subcategory_key = category_key  # Assume subcategory is same as category
            config_key = (category_key, subcategory_key)

            expected_title = sample_plot_configs.get(config_key, {}).get("title", "")
            assert (
                content_title == expected_title
            ), f"Content title does not match for {option_name}"

            # Interact with elements on the page as needed
            components_expected = sample_plot_configs.get(config_key, {}).get(
                "components", []
            )

            for component in components_expected:
                if component["type"] == "plot":
                    # Verify the plot is rendered
                    try:
                        plot_element = wait.until(
                            EC.presence_of_element_located((By.ID, component["id"]))
                        )
                        assert (
                            plot_element.is_displayed()
                        ), f"Plot '{component['id']}' is not displayed in {option_name}"
                    except Exception as e:
                        raise Exception(
                            f"Plot component with ID '{component['id']}' not found in {option_name}: {str(e)}"
                        )
                elif component["type"] == "table":
                    # Verify the table is rendered
                    try:
                        table_element = wait.until(
                            EC.presence_of_element_located((By.ID, component["id"]))
                        )
                        assert (
                            table_element.is_displayed()
                        ), f"Table '{component['id']}' is not displayed in {option_name}"

                        # Extract table data from the DOM
                        table_rows = table_element.find_elements(
                            By.CSS_SELECTOR, 'div[role="row"]'
                        )
                        # Subtract one for header row
                        assert len(table_rows) - 1 == len(
                            component["data"]
                        ), f"Number of table rows does not match in {option_name}"

                        # Verify table headers
                        header_cells = table_rows[0].find_elements(
                            By.CSS_SELECTOR, 'div[role="columnheader"]'
                        )
                        header_texts = [cell.text for cell in header_cells]
                        assert (
                            header_texts == component["columns"]
                        ), f"Table headers do not match in {option_name}"

                        # Verify table data
                        for i, row_data in enumerate(component["data"], start=1):
                            row_cells = table_rows[i].find_elements(
                                By.CSS_SELECTOR, 'div[role="gridcell"]'
                            )
                            cell_texts = [cell.text for cell in row_cells]
                            expected_texts = [
                                str(row_data[col]) for col in component["columns"]
                            ]
                            assert (
                                cell_texts == expected_texts
                            ), f"Row {i} data does not match in {option_name}"
                    except Exception as e:
                        raise Exception(
                            f"Table component with ID '{component['id']}' not found or invalid in {option_name}: {str(e)}"
                        )

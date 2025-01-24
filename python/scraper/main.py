from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
import pandas as pd

# Set up Selenium WebDriver
options = webdriver.ChromeOptions()
# options.add_argument('--headless')  # Run in headless mode (no GUI)
options.add_argument('--no-sandbox')
options.add_argument('--disable-dev-shm-usage')
options.add_argument('--disable-gpu')  # Disable GPU hardware acceleration
options.add_argument('--log-level=3')  # Suppress logging

# Initialize the WebDriver
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

# Target URL
url = 'https://dexcheck.ai/app/eth/top-crypto-traders'

try:
    # Access the page
    driver.get(url)

    # Wait for the table to load
    WebDriverWait(driver, 30).until(EC.presence_of_element_located((By.TAG_NAME, 'td')))

    # Get the page source after the table has loaded
    page_source = driver.page_source

    # Use BeautifulSoup to parse the HTML
    soup = BeautifulSoup(page_source, 'html.parser')

    # Locate the table (modify the selector based on actual HTML structure)
    table = soup.find('table')  # Adjust if needed
    if not table:
        print("Table not found on the page.")
        exit()

    # Extract table headers
    headers = [header.text.strip() for header in table.find_all('th')]
    print(f'Headers: {headers}')

    # Extract table rows
    data = []
    rows = table.find_all('tr')
    for row in rows[1:]:  # Skip the header row
        columns = row.find_all('td')
        row_data = [column.text.strip() for column in columns]
        data.append(row_data)
        print(f'Row data: {row_data}')

    # Create a DataFrame and save to CSV
    df = pd.DataFrame(data, columns=headers)
    df.to_csv('crypto_traders.csv', index=False, encoding='utf-8')
    print("Data saved to 'crypto_traders.csv'.")

except Exception as e:
    print(f"An error occurred: {e}")

finally:
    # Close the WebDriver
    driver.quit()

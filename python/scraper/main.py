from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
from prisma import Prisma

async def store_traders_data():
    # Initialize Prisma client
    prisma = Prisma()
    await prisma.connect()

    # Set up Selenium WebDriver
    options = webdriver.ChromeOptions()
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-gpu')
    options.add_argument('--log-level=3')

    # Initialize the WebDriver
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

    try:
        # Access the page
        driver.get('https://dexcheck.ai/app/eth/top-crypto-traders')

        # Wait for the table to load
        WebDriverWait(driver, 30).until(EC.presence_of_element_located((By.TAG_NAME, 'td')))
        
        # Get the page source after the table has loaded
        page_source = driver.page_source
        soup = BeautifulSoup(page_source, 'html.parser')
        
        # Locate the table
        table = soup.find('table')
        if not table:
            print("Table not found on the page.")
            return

        # Extract table rows
        rows = table.find_all('tr')
        for row in rows[1:]:  # Skip header row
            columns = row.find_all('td')
            if len(columns) >= 5:  # Ensure we have all required columns
                # Create trader record in database
                await prisma.trader.create({
                    'data': {
                        'address': columns[0].text.strip(),
                        'pnl': columns[1].text.strip(),
                        'trades': int(columns[2].text.strip()),
                        'winRate': columns[3].text.strip(),
                        'avgRoi': columns[4].text.strip(),
                    }
                })
        
        print("Data successfully stored in database")

    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        driver.quit()
        await prisma.disconnect()

# Run the async function
if __name__ == "__main__":
    import asyncio
    asyncio.run(store_traders_data())

from bs4 import BeautifulSoup
from prisma import Prisma
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import json
import pandas as pd
import time

def load_cookies():
    try:
        with open('cookies.json', 'r') as file:
            return json.load(file)
    except FileNotFoundError:
        return None

def save_cookies(driver):
    cookies = driver.get_cookies()
    with open('cookies.json', 'w') as file:
        json.dump(cookies, file)

async def get_tokens():
    db = Prisma()
    await db.connect()
    tokens = await db.token.find_many(take=1)
    await db.disconnect()
    return tokens

def setup_driver():
    options = webdriver.ChromeOptions()
    
    # Add essential options
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-gpu')
    options.add_argument('--disable-extensions')
    options.add_argument('--no-first-run')
    options.add_argument('--start-maximized')
    
    # Add stealth options
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_experimental_option('excludeSwitches', ['enable-automation'])
    options.add_experimental_option('useAutomationExtension', False)
    
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    
    return driver

def extract_table_data(table):
    data = []
    rows = table.select('.custom-1nvxwu0')
    
    for row in rows:
        # Extract rank
        rank = row.select_one('.custom-q9k0mw').text.strip('#')
        
        # Extract wallet address from explorer link
        wallet_link = row.select_one('.custom-1dwgrrr a')
        if wallet_link:
            href = wallet_link['href']
            wallet = href.split('/')[-1]  # Gets the full address from the URL
        else:
            wallet = '-'
        
        # Extract bought amount
        bought_div = row.select('.custom-1o79wax')[0]
        bought_span = bought_div.select_one('.custom-6qd5i2, .custom-rcecxm')
        bought = bought_span.text if bought_span else '-'
        
        # Extract bought volume if exists
        bought_volume = ''
        if bought != '-':
            volume_span = bought_div.select_one('.custom-2ygcmq')
            bought_volume = volume_span.text if volume_span else ''
        
        # Extract sold amount
        sold_div = row.select('.custom-1o79wax')[1]
        sold_span = sold_div.select_one('.custom-6qd5i2, .custom-dv3t8y')
        sold = sold_span.text if sold_span else '-'
        
        # Extract sold volume if exists
        sold_volume = ''
        if sold != '-':
            volume_span = sold_div.select_one('.custom-2ygcmq')
            sold_volume = volume_span.text if volume_span else ''
        
        # Extract PNL
        pnl_element = row.select_one('.custom-1e9y0rl, .custom-1yklr7h')
        pnl = pnl_element.text if pnl_element else '-'

        # Extract unrealized value
        unrealized_div = row.select_one('.custom-1hd7h4r')
        unrealized_span = unrealized_div.select_one('.custom-6qd5i2')
        unrealized = unrealized_span.text if unrealized_span else unrealized_div.text

        # Extract balance
        balance_div = row.select_one('.custom-1cicvqe')
        balance_unknown = balance_div.select_one('.custom-sqw9c5')
        if balance_unknown:
            balance = balance_unknown.text
        else:
            balance_spans = balance_div.select('.custom-2ygcmq')
            if len(balance_spans) >= 2:
                balance = f"{balance_spans[0].text}/{balance_spans[1].text}"
            else:
                balance = '-'
        
        # Extract transactions count
        txns_text = row.select('.custom-13ppmr2')[0].text
        txns = txns_text.split('/')[1].strip().split()[0] if txns_text else '0'
        
        row_data = [rank, wallet, bought, bought_volume, sold, sold_volume, pnl, unrealized, balance, txns]
        data.append(row_data)
    
    return data

async def scrape_top_traders():
    tokens = await get_tokens()
    driver = setup_driver()
    all_traders_data = []
    periods = ['30d', '7d', '3d', '1d']

    try:
        for token in tokens:
            cookies = load_cookies()
            url = f"https://dexscreener.com/{token.chain.lower()}/{token.address}"
            driver.get(url)

            if cookies:
                driver.delete_all_cookies()
                for cookie in cookies:
                    driver.add_cookie(cookie)
                driver.refresh()
            
            wait = WebDriverWait(driver, 60)
            wait.until(lambda d: d.execute_script('return document.readyState') == 'complete')
            
            # Scroll multiple times with longer pauses
            scroll_height = 0
            for _ in range(10):
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(5)
                
                new_height = driver.execute_script("return document.body.scrollHeight")
                if new_height == scroll_height:
                    break
                scroll_height = new_height
            
            # Wait for and click the Top Traders button
            wait = WebDriverWait(driver, 10)
            top_traders_button = wait.until(
                EC.element_to_be_clickable((By.CLASS_NAME, "custom-tv0t33"))
            )
            top_traders_button.click()
            
            # Wait for table to load
            time.sleep(5)
            
            # Update the period button click part in scrape_top_traders function
            for period in periods:
                # Find period button
                period_button = wait.until(
                    EC.presence_of_element_located((By.XPATH, f"//button[.//span[text()='{period}']]"))
                )
                # Use JavaScript to click the button
                driver.execute_script("arguments[0].click();", period_button)
                time.sleep(3)
                
                # Find the table
                table = wait.until(
                    EC.presence_of_element_located((By.CLASS_NAME, "custom-1vjv7zm"))
                )
                
                # Parse table with BeautifulSoup
                soup = BeautifulSoup(table.get_attribute('innerHTML'), 'html.parser')
                traders_data = extract_table_data(soup)
                
                # Add token info and period to each row
                for row in traders_data:
                    row.extend([token.address, token.token, token.chain, period])
                    all_traders_data.append(row)
                
            print(f"Processed token: {token.token}")
            time.sleep(2)


    finally:
        save_cookies(driver)
        driver.quit()

     # Create DataFrame and save to CSV
    columns = ['Rank', 'Wallet', 'Bought Amount', 'Bought Volume', 'Sold Amount', 'Sold Volume', 
              'PNL', 'Unrealized', 'Balance', 'Transactions', 'Token Address', 'Token Symbol', 
              'Chain', 'Period']
    df = pd.DataFrame(all_traders_data, columns=columns)
    df.to_csv('toptraders.csv', index=False)
    print("Data saved to toptraders.csv")

if __name__ == "__main__":
    import asyncio
    asyncio.run(scrape_top_traders())

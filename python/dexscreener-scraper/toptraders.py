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
    tokens = await db.token.find_many()
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

def convert_to_number(value):
    if value == '-' or value == 'Unknown':
        return '0'
    # Remove $, commas, and < symbols
    value = value.replace('$', '').replace(',', '').replace('<', '')
    
    # Handle K, M, B multipliers
    multipliers = {
        'K': 1000,                    # Thousand
        'M': 1000000,                 # Million
        'B': 1000000000,              # Billion
        'T': 1000000000000,           # Trillion
        'Q': 1000000000000000,        # Quadrillion
        'Qi': 1000000000000000000,    # Quintillion
        'Sx': 1000000000000000000000  # Sextillion
    }
    for suffix, multiplier in multipliers.items():
        if suffix in value:
            number = float(value.replace(suffix, '')) * multiplier
            return str(number)
    return value

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
        bought = convert_to_number(bought_span.text) if bought_span else '0'
        
        # Extract bought volume
        if bought != '0':
            volume_span = bought_div.select_one('.custom-2ygcmq')
            bought_volume = convert_to_number(volume_span.text) if volume_span else '0'
        else:
            continue

        # Extract sold amount
        sold_div = row.select('.custom-1o79wax')[1]
        sold_span = sold_div.select_one('.custom-6qd5i2, .custom-dv3t8y')
        sold = convert_to_number(sold_span.text) if sold_span else '0'
        
        # Extract sold volume
        if sold != '0':
            volume_span = sold_div.select_one('.custom-2ygcmq')
            sold_volume = convert_to_number(volume_span.text) if volume_span else '0'
        else:
            sold_volume = '0'
        
        # Extract PNL with sign
        pnl_element = row.select_one('.custom-1e9y0rl, .custom-1yklr7h')
        if pnl_element:
            # Convert value to number first
            raw_value = float(convert_to_number(pnl_element.text))
            # custom-1e9y0rl is positive, custom-1yklr7h is negative
            pnl = str(raw_value) if 'custom-1e9y0rl' in pnl_element.get('class', []) else str(-raw_value)
        else:
            pnl = '0'

        # Extract unrealized value
        unrealized_div = row.select_one('.custom-1hd7h4r')
        unrealized_span = unrealized_div.select_one('.custom-6qd5i2')
        unrealized = convert_to_number(unrealized_span.text if unrealized_span else unrealized_div.text)

        # Extract balance
        balance_div = row.select_one('.custom-1cicvqe')
        balance_unknown = balance_div.select_one('.custom-sqw9c5')
        if balance_unknown:
            balance = '0'
        else:
            balance_spans = balance_div.select('.custom-2ygcmq')
            if len(balance_spans) >= 2:
                current_balance = convert_to_number(balance_spans[0].text)
                total_supply = convert_to_number(balance_spans[1].text)
                balance = f"{current_balance}/{total_supply}"
            else:
                balance = '0'
        
       # Extract transactions count
        txns_elements = row.select('.custom-13ppmr2')
        if txns_elements:
            txns_text = txns_elements[0].text
            txns = txns_text.split('/')[1].strip().split()[0] if txns_text else '0'
        else:
            txns = '0'
        
        row_data = [rank, wallet, bought, bought_volume, sold, sold_volume, pnl, unrealized, balance, txns]
        data.append(row_data)
    
    return data

async def store_to_database(traders_data):
    db = Prisma()
    await db.connect()

    for row in traders_data:
        try:
            await db.toptrader.upsert(
                where={
                    'tokenAddress_period_rank': {
                        'tokenAddress': row[0],
                        'period': row[1],
                        'rank': int(row[2])
                    }
                },
                data={
                    'create': {
                        'tokenAddress': row[0],
                        'period': row[1],
                        'rank': int(row[2]),
                        'wallet': row[3],
                        'boughtAmount': float(row[4]),
                        'boughtVolume': float(row[5]),
                        'soldAmount': float(row[6]),
                        'soldVolume': float(row[7]),
                        'pnl': float(row[8]),
                        'unrealized': row[9],
                        'balance': row[10],
                        'transactions': row[11]
                    },
                    'update': {
                        'wallet': row[3],
                        'boughtAmount': float(row[4]),
                        'boughtVolume': float(row[5]),
                        'soldAmount': float(row[6]),
                        'soldVolume': float(row[7]),
                        'pnl': float(row[8]),
                        'unrealized': row[9],
                        'balance': row[10],
                        'transactions': row[11]
                    }
                }
            )
        except Exception as e:
            print(f"Error storing trader data: {str(e)}")

    await db.disconnect()

async def scrape_top_traders():
    tokens = await get_tokens()
    driver = setup_driver()
    periods = ['30d', '7d', '3d', '1d']
    all_traders_data = []
    max_retries = 3
    retry_delay = 10

    start_time = time.time()
    total_tokens = len(tokens)

    cookies = load_cookies()

    if tokens:
        url = f"https://dexscreener.com/{tokens[0].chain.lower()}/{tokens[0].address}"
        driver.get(url)
    
        if cookies:
            for cookie in cookies:
                driver.add_cookie(cookie)

    try:
        for index, token in enumerate(tokens, 1):
            token_start_time = time.time()
            token_traders_data = []
            retries = 0
            while retries < max_retries:
                try:
                    url = f"https://dexscreener.com/{token.chain.lower()}/{token.address}"
                    driver.get(url)

                    wait = WebDriverWait(driver, 60)
                    wait.until(lambda d: d.execute_script('return document.readyState') == 'complete')
                    break
                
                except Exception as e:
                    retries += 1
                    if retries == max_retries:
                        print(f"Failed to process token {token.token} after {max_retries} attempts")
                        continue
                    print(f"Retry {retries}/{max_retries} for token {token.token}")
                    time.sleep(retry_delay)
            
            # Scroll multiple times with longer pauses
            scroll_height = 0
            for _ in range(10):
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(2)
                
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
            time.sleep(2)
            
            # Update the period button click part in scrape_top_traders function
            for period in periods:
                if period != '30d':
                    # Find period button
                    period_button = wait.until(
                        EC.presence_of_element_located((By.XPATH, f"//button[.//span[text()='{period}']]"))
                    )
                    # Use JavaScript to click the button
                    driver.execute_script("arguments[0].click();", period_button)
                    time.sleep(2)
                
                # Find the table
                table = wait.until(
                    EC.presence_of_element_located((By.CLASS_NAME, "custom-1vjv7zm"))
                )
                
                # Parse table with BeautifulSoup
                soup = BeautifulSoup(table.get_attribute('innerHTML'), 'html.parser')
                traders_data = extract_table_data(soup)
                
                # Add token info and period to each row
                for row in traders_data:
                    row_data = [token.address, period] + row
                    token_traders_data.append(row_data)
                
            # Store data for this token immediately
            all_traders_data.extend(token_traders_data)
                
            token_time = time.time() - token_start_time
            total_time = time.time() - start_time
            print(f"Token {index}/{total_tokens}: {token.token} | Time for token: {token_time:.2f}s | Total time: {total_time:.2f}s")
            time.sleep(2)

    finally:
        save_cookies(driver)
        driver.quit()
    
    # Store all data in the database
    batch_size = 100
    for i in range(0, len(all_traders_data), batch_size):
        batch = all_traders_data[i:i + batch_size]
        await store_to_database(batch)

if __name__ == "__main__":
    import asyncio
    asyncio.run(scrape_top_traders())

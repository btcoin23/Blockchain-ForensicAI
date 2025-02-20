from prisma import Prisma
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import pandas as pd
import os
import time
import logging
import json
from datetime import datetime
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

from seleniumbase import Driver

def setup_driver():
    options = webdriver.ChromeOptions()

    # options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-gpu')
    options.add_argument('--window-size=1920,1080')
    options.add_argument('--disable-blink-features=AutomationControlled')

    options.page_load_strategy = 'eager'

    service = Service(ChromeDriverManager().install())
    driver = Driver(uc=True)
    
    return driver

def setup_logging():
    if not os.path.exists('logs'):
        os.makedirs('logs')
        
    log_filename = f'logs/gmgn_scraper_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)


def wait_for_element(driver, selector, by=By.CSS_SELECTOR, timeout=10):
    return WebDriverWait(driver, timeout).until(
        EC.presence_of_element_located((by, selector))
    )

def convert_to_number(value):
    # Remove any leading '+' sign
    value = value.replace('+', '')
    
    # Handle K (thousands)
    if 'K' in value:
        return float(value.replace('K', '')) * 1000
    # Handle M (millions) if needed
    elif 'M' in value:
        return float(value.replace('M', '')) * 1000000
    else:
        return float(value)

def click_time_filter(driver, period, logger):
    logger.info(f"Attempting to click {period} filter button")
    
    # Wait for page load
    time.sleep(10)
    
    # CSS selectors for PnL buttons
    button_selector = "div.TableMultipleSort_item__QC9gV"
    pnl_text = {'Daily': '1D PnL', 'Weekly': '7D PnL', 'Monthly': '30D PnL'}
    
    def find_and_click_button():
        WebDriverWait(driver, 20).until(
            EC.presence_of_all_elements_located((By.CSS_SELECTOR, button_selector))
        )
        
        # Find all buttons again to avoid stale elements
        buttons = driver.find_elements(By.CSS_SELECTOR, button_selector)
        print(f"Found {len(buttons)} buttons")
        
        # Find the button with matching text
        for button in buttons:
            try:
                if pnl_text[period] in button.text:
                    initial_data = driver.page_source

                    button.click()
                    time.sleep(5) 
                    button.click()
                    if driver.page_source != initial_data:
                        logger.info(f"Successfully clicked and verified {period} PnL button")
                        return True
            except:
                continue
        return False
    
    max_attempts = 3
    for attempt in range(max_attempts):
        try:
            if find_and_click_button():
                return
            logger.warning(f"Button click attempt {attempt + 1} failed, retrying...")
            time.sleep(2)
        except Exception as e:
            logger.warning(f"Attempt {attempt + 1} failed: {str(e)}")
            time.sleep(2)
    
    raise Exception(f"Failed to click {period} button after {max_attempts} attempts")


def click_svg_icon(driver, logger):
    logger.info("Attempting to click SVG icon")
    
    # Single, specific selector targeting the SVG within the chakra-portal structure
    svg_selector = "//div[contains(@class, 'chakra-portal')]//div[contains(@class, 'css-12rtj2z')]//div[contains(@class, 'css-pt4g3d')]"
    
    try:
        svg_element = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, svg_selector))
        )
        driver.execute_script("arguments[0].click();", svg_element)
        logger.info("Successfully clicked SVG icon")
        return True
            
    except Exception as e:
        logger.error(f"Failed to click SVG icon: {str(e)}")
        return False


def extract_data(driver, period, logger):
    # Add wait for table to load
    time.sleep(15)  # Give more time for data to populate

    period_days = {'Daily': 1, 'Weekly': 7, 'Monthly': 30}
    period_index = list(period_days.keys()).index(period)

    # Wait for table rows to be present
    try:
        WebDriverWait(driver, 20).until(
            EC.presence_of_all_elements_located((By.CSS_SELECTOR, 'tr.g-table-row.g-table-row-level-0'))
        )
    except Exception as e:
        logger.warning(f"Timeout waiting for table rows: {str(e)}")
    
    soup = BeautifulSoup(driver.page_source, 'html.parser')

    users = soup.select('tr.g-table-row.g-table-row-level-0')

    # with open('gmgn_users.html', 'w', encoding='utf-8') as f:
    #     for user in users:
    #         f.write(str(user) + '\n')
    data = []

    for user in users:
        try:
            # Get wallet info
            wallet_cell = user.select_one('td.g-table-cell-fix-left')
            wallet_name = wallet_cell.select_one('a.css-f8qc29').text.strip()
            wallet_address = wallet_cell.select_one('a.css-1y09dqu')['href'].split('/address/')[1]
            
            twitter_link = wallet_cell.select_one('a.css-759u60')
            twitter = twitter_link['href'] if twitter_link else None
            
            # Get PNL data
            pnl_cell = user.select('td.g-table-cell')[period_index + 1]

            pnl_values = pnl_cell.select('p.chakra-text')
            if pnl_values and len(pnl_values) >= 2:
                pnl_percentage = pnl_values[0].text.replace('%', '').strip().replace(',','')
                pnl_usd = convert_to_number(pnl_values[1].text.replace('$', '').strip().replace(',',''))

            else:
                pnl_percentage = '0'
                pnl_usd = '0'

            # Get win/loss data
            stats_cell = user.select('td.g-table-cell')[5]
            win_loss = stats_cell.select('p.chakra-text')
            win = win_loss[1].text.strip().replace(',','')
            loss = win_loss[2].text.strip().replace(',','')
            
            data.append({
                'period': period_days[period],
                'wallet_name': wallet_name,
                'wallet_address': wallet_address,
                'win': win,
                'loss': loss,
                'pnl_percentage': pnl_percentage,
                'pnl_usd': pnl_usd,
                'telegram': None,
                'twitter': twitter
            })
            
        except Exception as e:
            logger.warning(f"Failed to extract data for a user: {str(e)}")
            continue

    logger.info(f"Successfully extracted data for {len(data)} users")
    return data


async def save_to_database(data):
    db = Prisma()
    await db.connect()

    for record in data:
        try:
            await db.gmgnkol.upsert(
                where={
                    'id': record.get('id', 0)
                },
                data={
                    'create': {
                        'period': record['period'],
                        'wallet_name': record['wallet_name'],
                        'wallet_address': record['wallet_address'],
                        'pnl_percentage': record['pnl_percentage'],
                        'pnl_usd': float(record['pnl_usd']),
                        'telegram': record['telegram'],
                        'twitter': record['twitter'],
                        'win': int(record['win']),
                        'loss': int(record['loss'])
                    },
                    'update': {
                        'period': record['period'],
                        'wallet_name': record['wallet_name'],
                        'pnl_percentage': record['pnl_percentage'],
                        'pnl_usd': float(record['pnl_usd']),
                        'telegram': record['telegram'],
                        'twitter': record['twitter'],
                        'win': int(record['win']),
                        'loss': int(record['loss'])
                    }
                }
            )
        except Exception as e:
            print(f"Error storing record for {record['wallet_address']}: {str(e)}")

    await db.disconnect()
    print(f"Saved {len(data)} records to database")


# def save_to_csv(data):
#     df = pd.DataFrame(data)
#     filename = f'kol_gmgn.csv'
#     df.to_csv(filename, index=False)
#     print(f"Saved {filename} with {len(data)} records")

async def scrape_gmgn():
    logger = setup_logging()
    logger.info("Initializing scraper")
    
    driver = setup_driver()
    logger.info("Browser driver setup complete")
    
    all_data = []  
    
    try:
        logger.info("Navigating to GMGN leaderboard")
        driver.get("https://gmgn.ai/trade?chain=sol&tab=renowned")
        logger.info("Page loaded, waiting for initial render")
        time.sleep(5) 
        click_svg_icon(driver, logger)
        time.sleep(2) 

        for period in ['Daily', 'Weekly', 'Monthly']:
            try:
                logger.info(f"=== Starting {period} period scraping ===")
                click_time_filter(driver, period, logger)
                period_data = extract_data(driver, period, logger)
                all_data.extend(period_data)  # Add period data to all_data
            except Exception as e:
                logger.error(f"Failed to complete {period} scraping: {str(e)}", exc_info=True)

        await save_to_database(all_data)  # Save combined data from all periods
    
    except Exception as e:
        logger.error(f"Critical scraper error: {str(e)}", exc_info=True)
    
    finally:      
        driver.quit()
        logger.info("Scraping process completed, browser closed")

if __name__ == "__main__":
    import asyncio
    asyncio.run(scrape_gmgn())

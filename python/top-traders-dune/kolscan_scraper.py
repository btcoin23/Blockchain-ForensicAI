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
import re
from datetime import datetime
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager


def setup_driver():
    options = webdriver.ChromeOptions()

    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-gpu')
    options.add_argument('--window-size=1920,1080')
    options.add_argument('--disable-blink-features=AutomationControlled')

    service = Service(ChromeDriverManager().install())
    return webdriver.Chrome(service=service, options=options)


# Set up logging configuration at the top of your script
def setup_logging():
    # Create logs directory if it doesn't exist
    if not os.path.exists('logs'):
        os.makedirs('logs')
        
    # Set up logging with timestamp in filename
    log_filename = f'logs/kolscan_scraper_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
    
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


def click_time_filter(driver, period, logger):
    logger.info(f"Attempting to click {period} filter button")
    
    # Wait for page load
    time.sleep(1)
    
    # Target buttons using exact class names from the HTML
    button_selectors = {
        'Daily': "//div[@class='leaderboard_timeFilterContainer__9U8_y']/p[@class='leaderboard_selected__q7DOH']",
        'Weekly': "//div[@class='leaderboard_timeFilterContainer__9U8_y']/p[text()='Weekly']",
        'Monthly': "//div[@class='leaderboard_timeFilterContainer__9U8_y']/p[text()='Monthly']"
    }
    
    try:
        # Find and click the button
        button = WebDriverWait(driver, 20).until(
            EC.element_to_be_clickable((By.XPATH, button_selectors[period]))
        )
        driver.execute_script("arguments[0].click();", button)
        logger.info(f"Successfully clicked {period} button")
        time.sleep(2)
        
    except Exception as e:
        logger.error(f"Failed to click {period} button: {str(e)}")
        raise

def extract_data(driver, period, logger):
    period_days = {'Daily': 1, 'Weekly': 7, 'Monthly': 30}
    soup = BeautifulSoup(driver.page_source, 'html.parser')

    scripts = soup.find_all('script')
    combined_push_content = ''

    for script in scripts:
        if script.string and 'self.__next_f.push' in script.string:
            cleaned_content = script.string.replace('self.__next_f.push([', '').replace(']);', '')
            if cleaned_content.startswith('1,'):
                cleaned_content = cleaned_content[2:]
            combined_push_content += cleaned_content.replace('])', '')

    combined_push_content = combined_push_content.replace('\\', '').split('"initialData":')[1].split('"initialUserData":')[0]
    combined_push_content = combined_push_content.replace(',"telegram":""', ',"telegram":null').replace('""', '')
    combined_push_content = combined_push_content.rstrip(',')

    # Convert combined_push_content to a Python list and create lookup dictionary
    try:
        combined_data = json.loads('[' + combined_push_content + ']')
        social_lookup = {
            item['wallet_address']: (item.get('telegram'), item.get('twitter'))
            for item in combined_data[0]  # Access the first element since combined_data is a list
        }

    except (json.JSONDecodeError, KeyError, IndexError) as e:
        logger.error(f"Failed to parse JSON data: {str(e)}")
        social_lookup = {}

    users = soup.find_all('div', class_='leaderboard_leaderboardUser__8OZpJ')
    data = []

    for user in users:
        try:
            account_link = user.find('a', style="display:flex;align-items:center;gap:10px;white-space:nowrap")
            wallet_name = account_link.find('h1').text.strip()
            wallet_address = account_link['href'].split('/account/')[1]

            win_div_tags = user.find('div', class_='remove-mobile').find_all('p')
            win = int(win_div_tags[0].text.strip())
            loss = int(win_div_tags[1].text.strip())

            pnl_div = user.find('div', class_='leaderboard_totalProfitNum__HzfFO')
            h1_tags = pnl_div.find_all('h1')

            pnl_sol = h1_tags[0].text.replace('Sol', '').strip()
            pnl_usd = h1_tags[1].text.replace(',', '').replace('$', '').replace('(', '').replace(')', '').strip()

            telegram, twitter = social_lookup.get(wallet_address, (None, None))

            data.append({
                'period': period_days[period],
                'wallet_name': wallet_name,
                'wallet_address': wallet_address,
                'win': win,
                'loss': loss,
                'pnl_usd': pnl_usd,
                'pnl_sol': pnl_sol,
                'telegram': telegram,
                'twitter': twitter
            })

        except AttributeError as e:
            logger.warning(f"Failed to extract data for a user: {str(e)}")
            continue

    logger.info(f"Successfully extracted data for {len(data)} users")
    return data

async def save_to_database(data):
    db = Prisma()
    await db.connect()

    for record in data:
        try:
            await db.kolleaderboard.upsert(
                where={
                    'id': record.get('id', 0)  # Using id as the unique identifier
                },
                data={
                    'create': {
                        'period': record['period'],
                        'wallet_name': record['wallet_name'],
                        'wallet_address': record['wallet_address'],
                        'win':record['win'],
                        'loss':record['loss'],
                        'pnl_usd': record['pnl_usd'],
                        'pnl_sol': record['pnl_sol'],
                        'telegram': record['telegram'],
                        'twitter': record['twitter']
                    },
                    'update': {
                        'period': record['period'],
                        'wallet_name': record['wallet_name'],
                        'win':record['win'],
                        'loss':record['loss'],
                        'pnl_usd': record['pnl_usd'],
                        'pnl_sol': record['pnl_sol'],
                        'telegram': record['telegram'],
                        'twitter': record['twitter']
                    }
                }
            )
        except Exception as e:
            print(f"Error storing record for {record['wallet_address']}: {str(e)}")

    await db.disconnect()
    print(f"Saved {len(data)} records to database")


async def scrape_kolscan():
    logger = setup_logging()
    logger.info("Initializing scraper")
    
    driver = setup_driver()
    logger.info("Browser driver setup complete")
    
    all_data = []  # Initialize list to store data from all periods
    
    try:
        logger.info("Navigating to KOLscan leaderboard")
        driver.get("https://kolscan.io/leaderboard")
        logger.info("Page loaded, waiting for initial render")
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
    asyncio.run(scrape_kolscan())

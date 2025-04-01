from prisma import Prisma
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import os
import time
import logging
from datetime import datetime
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from seleniumbase import Driver

def setup_driver():
    options = webdriver.ChromeOptions()
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-gpu')
    options.add_argument('--window-size=1920,1080')
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.page_load_strategy = 'eager'
    
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

def wait_for_element(driver, selector, by=By.CSS_SELECTOR, timeout=5):
    return WebDriverWait(driver, timeout).until(
        EC.presence_of_element_located((by, selector))
    )

def convert_to_number(value):
    value = value.replace('+', '')
    
    if 'K' in value:
        return float(value.replace('K', '')) * 1000
    elif 'M' in value:
        return float(value.replace('M', '')) * 1000000
    else:
        return float(value)

def click_time_filter(driver, period, logger):
    logger.info(f"Attempting to click {period} filter button")
    
    # Reduced wait time
    time.sleep(3)
    
    button_selector = "div.TableMultipleSort_item__r8sgn"
    pnl_text = {'Daily': '1D PnL', 'Weekly': '7D PnL', 'Monthly': '30D PnL'}
    
    def find_and_click_button():
        WebDriverWait(driver, 10).until(
            EC.presence_of_all_elements_located((By.CSS_SELECTOR, button_selector))
        )
        
        buttons = driver.find_elements(By.CSS_SELECTOR, button_selector)
        print(f"Found {len(buttons)} buttons")
        
        for button in buttons:
            try:
                if pnl_text[period] in button.text:
                    initial_data = driver.page_source
                    button.click()
                    time.sleep(2)  # Reduced wait time
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
            time.sleep(1)  # Reduced wait time
        except Exception as e:
            logger.warning(f"Attempt {attempt + 1} failed: {str(e)}")
            time.sleep(1)  # Reduced wait time
    
    raise Exception(f"Failed to click {period} button after {max_attempts} attempts")

def click_svg_icon(driver, logger):
    logger.info("Attempting to click SVG icon")
    
    svg_selector = "//div[contains(@class, 'chakra-portal')]//div[contains(@class, 'css-12rtj2z')]//div[contains(@class, 'css-pt4g3d')]"
    
    try:
        svg_element = WebDriverWait(driver, 5).until(  # Reduced timeout
            EC.element_to_be_clickable((By.XPATH, svg_selector))
        )
        driver.execute_script("arguments[0].click();", svg_element)
        logger.info("Successfully clicked SVG icon")
        return True
            
    except Exception as e:
        logger.error(f"Failed to click SVG icon: {str(e)}")
        return False

def extract_data(driver, period, logger):
    # Reduced wait time but still giving enough time for data to load
    time.sleep(8)  

    period_days = {'Daily': 1, 'Weekly': 7, 'Monthly': 30}
    period_index = list(period_days.keys()).index(period)

    try:
        WebDriverWait(driver, 10).until(  # Reduced timeout
            EC.presence_of_all_elements_located((By.CSS_SELECTOR, 'tr.g-table-row.g-table-row-level-0'))
        )
    except Exception as e:
        logger.warning(f"Timeout waiting for table rows: {str(e)}")
    
    soup = BeautifulSoup(driver.page_source, 'html.parser')
    users = soup.select('tr.g-table-row.g-table-row-level-0')
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

async def save_to_database(data, logger):
    logger.info(f"Saving {len(data)} records to database")
    db = Prisma()
    await db.connect()
    
    success_count = 0
    error_count = 0

    for record in data:
        try:
            # Find existing record by wallet_address and period
            existing_record = await db.gmgnkol.find_first(
                where={
                    'wallet_address': record['wallet_address'],
                    'period': record['period']
                }
            )
            
            if existing_record:
                # Update existing record
                await db.gmgnkol.update(
                    where={
                        'id': existing_record.id
                    },
                    data={
                        'wallet_name': record['wallet_name'],
                        'pnl_percentage': record['pnl_percentage'],
                        'pnl_usd': float(record['pnl_usd']),
                        'telegram': record['telegram'],
                        'twitter': record['twitter'],
                        'win': int(record['win']),
                        'loss': int(record['loss'])
                    }
                )
            else:
                # Create new record
                await db.gmgnkol.create(
                    data={
                        'period': record['period'],
                        'wallet_name': record['wallet_name'],
                        'wallet_address': record['wallet_address'],
                        'pnl_percentage': record['pnl_percentage'],
                        'pnl_usd': float(record['pnl_usd']),
                        'telegram': record['telegram'],
                        'twitter': record['twitter'],
                        'win': int(record['win']),
                        'loss': int(record['loss'])
                    }
                )
            
            success_count += 1
        except Exception as e:
            logger.error(f"Error storing record for {record['wallet_address']}: {str(e)}")
            error_count += 1

    await db.disconnect()
    logger.info(f"Database operation completed: {success_count} records saved, {error_count} errors")
    return success_count, error_count

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
        time.sleep(3)  # Reduced wait time
        click_svg_icon(driver, logger)
        time.sleep(1)  # Reduced wait time

        for period in ['Daily', 'Weekly', 'Monthly']:
            try:
                logger.info(f"=== Starting {period} period scraping ===")
                click_time_filter(driver, period, logger)
                period_data = extract_data(driver, period, logger)
                all_data.extend(period_data)
                logger.info(f"Collected {len(period_data)} records for {period} period")
            except Exception as e:
                logger.error(f"Failed to complete {period} scraping: {str(e)}", exc_info=True)
    
    except Exception as e:
        logger.error(f"Critical scraper error: {str(e)}", exc_info=True)
    
    finally:
        # Always save data before exiting, even if there was an error
        if all_data:
            logger.info(f"Saving all collected data ({len(all_data)} records) before exiting")
            success, errors = await save_to_database(all_data, logger)
            logger.info(f"Final database save: {success} successful, {errors} failed")
        else:
            logger.warning("No data collected to save")
            
        driver.quit()
        logger.info("Scraping process completed, browser closed")

if __name__ == "__main__":
    import asyncio
    asyncio.run(scrape_gmgn())

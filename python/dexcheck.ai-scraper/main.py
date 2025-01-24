from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import pandas as pd
import json
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

def scrape_data():
    driver = setup_driver()
    url = 'https://dexscreener.com/?rankBy=trendingScoreM5&order=desc'

    try:
        cookies = load_cookies()
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
        
        time.sleep(5)
        
        # Get only the main content
        main_content = driver.find_element(By.TAG_NAME, 'main').get_attribute('innerHTML')
        soup = BeautifulSoup(main_content, 'html.parser')
        
        table_container = soup.select_one('.ds-dex-table-top')

        # with open('table.html', 'w', encoding='utf-8') as f:
        #     f.write(str(table_container))
        
        if table_container:
            headers = table_container.select('.ds-table-th-button')
            # with open('headers.html', 'w', encoding='utf-8') as f:
            #     f.write(str(headers))

            header_texts = [header.text.strip() for header in headers if header.text.strip()]
            
            rows = table_container.select('.ds-dex-table-row')
            # with open('rows.html', 'w', encoding='utf-8') as f:
            #     f.write(str(rows))

            rows_data = []
            
            for row in rows:
                cells = row.select('.ds-table-data-cell')
                if cells:
                    row_data = [cell.text.strip() for cell in cells]
                    if any(row_data):
                        rows_data.append(row_data)
            
            # with open('rows_data.html', 'w', encoding='utf-8') as f:
            #     f.write(str(rows_data))

            if rows_data:
                df = pd.DataFrame(rows_data, columns=header_texts)
                df.to_csv('trending_tokens.csv', index=False, encoding='utf-8')
                print(f"Successfully extracted {len(rows_data)} rows")
            else:
                print("No data rows found in the table")
        else:
            print("Table container not found")
            
    except Exception as e:
        print(f"Extraction error: {str(e)}")
    finally:
        save_cookies(driver)
        driver.quit()

if __name__ == "__main__":
    scrape_data()

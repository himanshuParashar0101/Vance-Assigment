from datetime import datetime
import requests
from bs4 import BeautifulSoup
import sqlite3
from dotenv import load_dotenv
import os

load_dotenv()

DATABASE_PATH = os.getenv('DATABASE_PATH', 'yahoo_finance.db')

def create_table(table_name):
    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()
    cursor.execute(f'''
    CREATE TABLE IF NOT EXISTS {table_name} (
        date DATETIME UNIQUE,
        open REAL,
        high REAL,
        low REAL,
        close REAL,
        adj_close REAL,
        volume INTEGER
    )
    ''')
    conn.commit()
    conn.close()

def insert_data(table_name, data):
    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()
    cursor.executemany(f'''
    INSERT OR IGNORE INTO {table_name} (date, open, high, low, close, adj_close, volume) 
    VALUES (?, ?, ?, ?, ?, ?, ?)
    ''', data)
    conn.commit()
    conn.close()

def scrape_data(quote, from_date, to_date, from_currency, to_currency):
    url = f"https://finance.yahoo.com/quote/{quote}/history/?period1={from_date}&period2={to_date}&interval=1d"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36'
    }
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.text, 'html.parser')
    rows = []
    table = soup.find('table', class_='yf-ewueuo')
    if table:
        for row in table.find_all('tr')[1:]:
            cols = row.find_all('td')
            if len(cols) == 7:
                try:
                    date_obj = datetime.strptime(cols[0].text, "%b %d, %Y")
                    formatted_date = date_obj.strftime("%Y-%m-%d")
                    date = formatted_date
                    open_ = float(cols[1].text.replace(',', ''))
                    high = float(cols[2].text.replace(',', ''))
                    low = float(cols[3].text.replace(',', ''))
                    close = float(cols[4].text.replace(',', ''))
                    adj_close = float(cols[5].text.replace(',', ''))
                    volume = int(cols[6].text.replace(',', '')) if cols[6].text.strip() != '-' else None
                    rows.append((date, open_, high, low, close, adj_close, volume))
                except ValueError as e:
                    print(f"ValueError: {e}")
                    continue
    else:
        print("No data table found on the page.")
    return rows

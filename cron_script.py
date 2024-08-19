from datetime import datetime, timedelta
import os
import sqlite3
from backend.scraper import scrape_data, insert_data, create_table

def get_latest_date(table_name):
    conn = sqlite3.connect(os.getenv('DATABASE_PATH', 'yahoo_finance.db'))
    cursor = conn.cursor()
    cursor.execute(f"SELECT MAX(date) FROM {table_name}")
    result = cursor.fetchone()[0]
    conn.close()
    return datetime.strptime(result, "%Y-%m-%d") if result else None

def handle_scraping():
    currencies = [
        ('GBP', 'INR'),
        ('AED', 'INR')
    ]
    periods = { # Commented 6M, 3M, 1M, 1W since the data is already included in 1Y
        '1Y': timedelta(weeks=52),
        # '6M': timedelta(weeks=24),
        # '3M': timedelta(weeks=12),
        # '1M': timedelta(weeks=4),
        # '1W': timedelta(weeks=1),
    }
    
    print("******************Started CronJob******************")
    for from_currency, to_currency in currencies:
        for period, duration in periods.items():
            print(f"Task: {from_currency} to {to_currency} for {period}")
            table_name = f"{from_currency}{to_currency}"
            create_table(table_name)
            
            latest_date = get_latest_date(table_name)
            to_date = datetime.now()
            if latest_date:
                print(f"Data already exists, fetching only for latest date")
                from_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
            else:
                from_date = (to_date - duration).strftime("%Y-%m-%d")
            
            formatted_from_date = int(datetime.strptime(from_date, "%Y-%m-%d").timestamp())
            formatted_to_date = int(datetime.now().timestamp())
            
            quote = f"{from_currency}{to_currency}=X"
            
            print(f"Scraping data from {from_date} to {to_date.strftime('%Y-%m-%d')}")
            data = scrape_data(quote, formatted_from_date, formatted_to_date, from_currency, to_currency)
            insert_data(table_name, data)
    print("==================Ended job Successfully==================")

if __name__ == "__main__":
    handle_scraping()

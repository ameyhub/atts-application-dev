import requests
from bs4 import BeautifulSoup
import psycopg2
import time
from db_config import DB_CONFIG  # Import PostgreSQL config
from nse500_stock_list import nse500stocklist  # Import stock list
import re

# Function to sanitize table names
def get_table_name(stock_symbol):
    return f"stock_{stock_symbol}_fundamental" if stock_symbol[0].isdigit() else f"{stock_symbol}_fundamental"

# Function to create table if not exists
def create_table(stock_symbol):
    """Create table dynamically based on stock symbol."""
    table_name = get_table_name(stock_symbol)
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        create_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            stock_symbol TEXT PRIMARY KEY,
            market_cap NUMERIC,
            current_price NUMERIC,
            high_low NUMERIC,
            stock_pe NUMERIC,
            book_value NUMERIC,
            dividend_yield NUMERIC,
            roce NUMERIC,
            roe NUMERIC,
            face_value NUMERIC
        );
        """
        cursor.execute(create_query)
        conn.commit()
        cursor.close()
        conn.close()
        print(f"✅ Table '{table_name}' is ready.")
    except Exception as e:
        print(f"⚠️ Table creation error for {table_name}: {e}")

# Function to insert data into PostgreSQL
def insert_stock_data(stock_data):
    """Insert stock data into dynamically created table."""
    table_name = get_table_name(stock_data["Stock"])
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        insert_query = f"""
        INSERT INTO {table_name} 
        (stock_symbol, market_cap, current_price, high_low, stock_pe, book_value, 
         dividend_yield, roce, roe, face_value) 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (stock_symbol) DO UPDATE SET 
            market_cap = EXCLUDED.market_cap,
            current_price = EXCLUDED.current_price,
            high_low = EXCLUDED.high_low,
            stock_pe = EXCLUDED.stock_pe,
            book_value = EXCLUDED.book_value,
            dividend_yield = EXCLUDED.dividend_yield,
            roce = EXCLUDED.roce,
            roe = EXCLUDED.roe,
            face_value = EXCLUDED.face_value;
        """
        cursor.execute(insert_query, (
            stock_data["Stock"], stock_data["Market Cap"], stock_data["Current Price"], 
            stock_data["High / Low"], stock_data["Stock P/E"], stock_data["Book Value"], 
            stock_data["Dividend Yield"], stock_data["ROCE"], stock_data["ROE"], stock_data["Face Value"]
        ))
        
        conn.commit()
        cursor.close()
        conn.close()
        print(f"✅ Inserted/Updated {stock_data['Stock']} in table {table_name} successfully.")
    except Exception as e:
        print(f"⚠️ Database Insert Error for {stock_data['Stock']} in {table_name}: {e}")

# Function to fetch stock data
def get_stock_data(stock_symbol):
    """Fetch stock data from Screener.in"""
    url = f"https://www.screener.in/company/{stock_symbol}/"
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"}
    
    try:
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code != 200:
            print(f"❌ Failed to fetch data for {stock_symbol} (Status: {response.status_code})")
            return None  

        soup = BeautifulSoup(response.text, "html.parser")
        
        data_points = [
            "Market Cap", "Current Price", "High / Low", "Stock P/E",
            "Book Value", "Dividend Yield", "ROCE", "ROE", "Face Value"
        ]

        stock_data = {"Stock": stock_symbol}
        for point in data_points:
            element = soup.select_one(f"#top-ratios li:has(span.name:-soup-contains('{point}')) span.number")
            stock_data[point] = element.text.strip() if element else "N/A"

        return stock_data
    except requests.RequestException as e:
        print(f"⚠️ Network error for {stock_symbol}: {e}")
        return None  

# Initialize stock list and storage
nse500_stock_list = nse500stocklist
failed_stocks = list(nse500_stock_list)  # Start with all stocks as failed

# Keep retrying until all stocks are fetched
while failed_stocks:
    print(f"\n🔄 Fetching data for {len(failed_stocks)} remaining stocks...\n")
    retry_failed_stocks = []

    for stock in failed_stocks:
        create_table(stock)  # Ensure table exists before inserting data
        stock_info = get_stock_data(stock)
        if stock_info:
            insert_stock_data(stock_info)  # Insert into PostgreSQL
        else:
            retry_failed_stocks.append(stock)  # Still failing

    failed_stocks = retry_failed_stocks  # Update failed stock list

    print(f"\n✅ Successfully inserted {len(nse500_stock_list) - len(failed_stocks)} stocks. {len(failed_stocks)} remaining...\n")
    
    if failed_stocks:
        print(f"🔄 Retrying {len(failed_stocks)} failed stocks in 10 seconds...\n")
        time.sleep(10)  # Wait before retrying to avoid IP bans

print("\n🎉 All 500 stocks successfully inserted/updated in PostgreSQL!\n")

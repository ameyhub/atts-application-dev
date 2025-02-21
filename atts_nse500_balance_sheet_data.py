import requests
from bs4 import BeautifulSoup
import psycopg2
from db_config import DB_CONFIG  # Database configuration
from nse500_stock_list import nse500stocklist  # List of stock symbols
import time

# Headers for HTTP requests
HEADERS = {"User-Agent": "Mozilla/5.0"}

# Required metrics (ensuring correct match with Screener)
REQUIRED_METRICS = {
    "Equity Capital": "Equity Capital",
    "Reserves": "Reserves",
    "Borrowings": "Borrowings",
    "Other Liabilities": "Other Liabilities",
    "Total Liabilities": "Total Liabilities",
    "Fixed Assets": "Fixed Assets",
    "CWIP": "CWIP",
    "Investments": "Investments",
    "Other Assets": "Other Assets",
    "Total Assets": "Total Assets"
}

# Function to clean numeric values
def clean_numeric(value):
    """Removes unwanted characters and converts to float."""
    if value == "-" or value is None:
        return None
    value = value.replace(",", "").replace("%", "").strip()
    try:
        return float(value)
    except ValueError:
        return None

# Function to scrape stock data
def scrape_stock_data(stock_symbol):
    url = f"https://www.screener.in/company/{stock_symbol}/"
    print(f"Fetching data for {stock_symbol}...")

    try:
        response = requests.get(url, headers=HEADERS)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"❌ Request failed for {stock_symbol}: {e}")
        return None, None

    soup = BeautifulSoup(response.text, "html.parser")
    balance_sheet_section = soup.find("section", {"id": "balance-sheet"})

    if not balance_sheet_section:
        print(f"No balance-sheet section found for {stock_symbol}")
        return None, None

    table = balance_sheet_section.find("table", {"class": "data-table"})
    if not table:
        print(f"No financial data found for {stock_symbol}")
        return None, None

    # Extract column headers (years)
    headers = [th.text.strip() for th in table.find("thead").find_all("th")]
    if len(headers) < 2:
        print(f"No valid yearly headers found for {stock_symbol}")
        return None, None

    yearly_headers = headers[1:]  # Skip first column (metric names)
    print(f"Extracted Years: {yearly_headers}")

    # Extract financial data rows
    rows = table.find("tbody").find_all("tr")
    financial_dict = {key: [None] * len(yearly_headers) for key in REQUIRED_METRICS}

    for row in rows:
        cols = row.find_all("td")
        metric_name = cols[0].get_text(strip=True)
        metric_name = " ".join(metric_name.split())  # Normalize spacing

        for key, value in REQUIRED_METRICS.items():
            if value.lower() in metric_name.lower():  # Case-insensitive matching
                values = [clean_numeric(col.get_text(strip=True)) for col in cols[1:]]
                financial_dict[key] = values
                break
        else:
            print(f"⚠️ Metric not matched: {metric_name}")

    # Format stock data for database storage
    stock_data = []
    for i, year in enumerate(yearly_headers):
        row_data = [year]
        for metric in REQUIRED_METRICS:
            row_data.append(financial_dict[metric][i] if i < len(financial_dict[metric]) else None)
        stock_data.append(row_data)

    return yearly_headers, stock_data

# Function to format table name
def format_table_name(stock_symbol):
    if stock_symbol[0].isdigit():
        return f"stock_{stock_symbol.lower()}_balance_sheet"
    return f"{stock_symbol.lower()}_balance_sheet"

# Function to create stock table
def create_stock_table(stock_symbol):
    table_name = format_table_name(stock_symbol)
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        id SERIAL PRIMARY KEY,
        yearly VARCHAR(20),
        equity_capital NUMERIC,
        reserves NUMERIC,
        borrowings NUMERIC,
        other_liabilities NUMERIC,
        total_liabilities NUMERIC,
        fixed_assets NUMERIC,
        cwip NUMERIC,
        investments NUMERIC,
        other_assets NUMERIC,
        total_assets NUMERIC
    );
    """
    cursor.execute(create_table_query)
    conn.commit()
    cursor.close()
    conn.close()

# Function to store data in PostgreSQL
def store_data_in_postgres(stock_symbol, data):
    if not data:
        return

    table_name = format_table_name(stock_symbol)
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    insert_query = f"""
    INSERT INTO {table_name} (
        yearly, equity_capital, reserves, borrowings, other_liabilities, 
        total_liabilities, fixed_assets, cwip, investments, other_assets, total_assets 
    ) VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """

    for row in data:
        row = [None if value == '-' else value for value in row]

        try:
            cursor.execute(insert_query, tuple(row))
        except Exception as e:
            print(f"❌ Error inserting data for {stock_symbol}: {row}\n{e}")
            conn.rollback()

    conn.commit()
    cursor.close()
    conn.close()
    print(f"✅ Data stored for {stock_symbol} in {table_name}!")

# Loop through stock symbols
for stock in nse500stocklist:
    create_stock_table(stock)
    yearly_headers, stock_data = scrape_stock_data(stock)
    if stock_data:
        store_data_in_postgres(stock, stock_data)
    time.sleep(2)

print("🎯 Data scraping and database storage completed successfully!")

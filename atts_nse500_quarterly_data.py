import requests
from bs4 import BeautifulSoup
import psycopg2
from db_config import DB_CONFIG  # Import database configuration
from nse500_stock_list import nse500stocklist  # Import stock symbols
import time

# Headers for web requests
headers = {"User-Agent": "Mozilla/5.0"}

# Required metrics including new fields
required_metrics = [
    "Sales+", "Revenue", "Expenses+", "Financing Profit", "Operating Profit", "Financing Margin %", "OPM %",  "Other Income+",
    "Interest", "Depreciation", "Profit before tax", "Tax %", "Net Profit+",
    "EPS in Rs", "Gross NPA %", "Net NPA %"
]

# Function to clean numeric values
def clean_numeric(value):
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
    response = requests.get(url, headers=headers)

    if response.status_code != 200:
        print(f"Failed to retrieve data for {stock_symbol}")
        return None

    soup = BeautifulSoup(response.text, "html.parser")
    table = soup.find("table", class_="data-table")
    if not table:
        print(f"No financial data found for {stock_symbol}")
        return None

    rows = table.find("tbody").find_all("tr")
    header_row = table.find("thead").find_all("th")[1:]
    quarters = [th.get_text(strip=True) for th in header_row]

    pdf_links = []
    pdf_section = soup.find("tr", class_="font-size-14 ink-600")
    if pdf_section:
        pdf_links = [
            "https://www.screener.in" + a["href"]
            for a in pdf_section.find_all("a", href=True)
        ]

    stock_data = []
    financial_dict = {metric: ["-"] * len(quarters) for metric in required_metrics}

    for row in rows:
        cols = row.find_all("td")
        metric_name = cols[0].get_text(strip=True)
        if metric_name in required_metrics:
            values = [col.get_text(strip=True) for col in cols[1:]]
            financial_dict[metric_name] = values

    for i, quarter in enumerate(quarters):
        row_data = [quarter]
        for metric in required_metrics:
            row_data.append(clean_numeric(financial_dict[metric][i]) if i < len(financial_dict[metric]) else None)
        row_data.append(pdf_links[i] if i < len(pdf_links) else None)
        stock_data.append(row_data)
    
    return stock_data

# Function to format table name
def format_table_name(stock_symbol):
    return f"stock_{stock_symbol.lower()}_quarterly" if stock_symbol[0].isdigit() else f"{stock_symbol.lower()}_quarterly"

# Function to create a table
def create_stock_table(stock_symbol):
    table_name = format_table_name(stock_symbol)
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        id SERIAL PRIMARY KEY,
        quarter VARCHAR(20),
        sales NUMERIC,
        revenue NUMERIC,
        expenses NUMERIC,
        financing_profit NUMERIC,
        operating_profit NUMERIC,
        financing_margin_percent NUMERIC,
        opm NUMERIC,
        other_income NUMERIC,
        interest NUMERIC,
        depreciation NUMERIC,
        profit_before_tax NUMERIC,
        tax NUMERIC,
        net_profit NUMERIC,
        eps NUMERIC,
        gross_npa_percent NUMERIC,
        net_npa_percent NUMERIC,
        raw_pdf_link TEXT
    );
    """
    cursor.execute(create_table_query)
    conn.commit()
    cursor.close()
    conn.close()

# Function to store data
def store_data_in_postgres(stock_symbol, data):
    if not data:
        return

    table_name = format_table_name(stock_symbol)
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    insert_query = f"""
    INSERT INTO {table_name} (
        quarter, sales, revenue, expenses, financing_profit, operating_profit, financing_margin_percent, opm, 
        other_income, interest, depreciation, profit_before_tax, tax, net_profit, eps, 
        gross_npa_percent, net_npa_percent, raw_pdf_link
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """

    for row in data:
        while len(row) < 18:
            row.append(None)
        
        if len(row) != 18:
            print(f"❌ Data mismatch for {stock_symbol}: {row}")
            continue  

        try:
            clean_row = [None if (val in ["", "-"]) else val for val in row]
            cursor.execute(insert_query, clean_row)
        except Exception as e:
            print(f"❌ Error inserting data for {stock_symbol}: {row}\n{e}")

    conn.commit()
    cursor.close()
    conn.close()
    print(f"✅ Data stored for {stock_symbol} in {table_name}!")

# Main execution
for stock in nse500stocklist:
    create_stock_table(stock)
    stock_data = scrape_stock_data(stock)
    if stock_data:
        store_data_in_postgres(stock, stock_data)
    time.sleep(2)

print("🎯 Data scraping and database storage completed successfully!")

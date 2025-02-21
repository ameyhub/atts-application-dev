import requests
from bs4 import BeautifulSoup
import psycopg2
from db_config import DB_CONFIG
from nse500_stock_list import nse500stocklist
import time

HEADERS = {"User-Agent": "Mozilla/5.0"}

REQUIRED_METRICS = {
    "Promoters": "Promoters",
    "FIIS": "FIIs",
    "DIIS": "DIIs",
    "Government": "Government",
    "Public": "Public",
    "No. of Shareholders": "No. of Shareholders"
}

def clean_numeric(value):
    if value in ("-", None):
        return None
    value = value.replace(",", "").replace("%", "").strip()
    try:
        return float(value)
    except ValueError:
        return None

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
    shareholding_section = soup.find("section", {"id": "shareholding"})

    if not shareholding_section:
        print(f"⚠️ No shareholding section found for {stock_symbol}")
        return None, None

    quarterly_data_section = shareholding_section.find("div", {"id": "quarterly-shp"})
    if not quarterly_data_section:
        print(f"⚠️ No quarterly shareholding data found for {stock_symbol}")
        return None, None

    table = quarterly_data_section.find("table", {"class": "data-table"})
    if not table:
        print(f"⚠️ No shareholding table found for {stock_symbol}")
        return None, None

    headers = [th.get_text(strip=True) for th in table.find("thead").find_all("th")] if table.find("thead") else []
    if len(headers) < 2:
        print(f"⚠️ No valid headers found for {stock_symbol}")
        return None, None

    yearly_headers = headers[1:]
    rows = table.find("tbody").find_all("tr") if table.find("tbody") else []
    
    financial_dict = {key: [None] * len(yearly_headers) for key in REQUIRED_METRICS}

    for row in rows:
        cols = row.find_all("td")
        if not cols:
            continue

        # Handling metric names with <button> inside <td>
        first_col = cols[0]
        metric_name = first_col.get_text(strip=True) if first_col else ""

        # Special handling for "Government" (extract from <button>)
        button = first_col.find("button")
        if button:
            metric_name = button.get_text(strip=True).split("+")[0].strip()

        # Debugging: Print extracted metric names
        print(f"Found metric: {metric_name}")

        for key, value in REQUIRED_METRICS.items():
            if value.lower() in metric_name.lower():
                values = [clean_numeric(col.get_text(strip=True)) if col else None for col in cols[1:]]
                financial_dict[key] = values
                break
        else:
            print(f"⚠️ Metric not matched: {metric_name}")

        # Special handling for "No. of Shareholders"
        if "No. of Shareholders" in metric_name:
            values = [clean_numeric(col.get_text(strip=True)) for col in cols[1:]]
            financial_dict["No. of Shareholders"] = values

    stock_data = []
    for i, year in enumerate(yearly_headers):
        row_data = [year]
        for metric in REQUIRED_METRICS:
            row_data.append(financial_dict[metric][i] if i < len(financial_dict[metric]) else None)
        stock_data.append(row_data)

    return yearly_headers, stock_data

def format_table_name(stock_symbol):
    table_name = stock_symbol.lower().replace("-", "_").replace(".", "_")
    if table_name[0].isdigit():
        table_name = "stock_" + table_name
    return f"{table_name}_shareholding_pattern"

def create_stock_table(stock_symbol):
    table_name = format_table_name(stock_symbol)
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        id SERIAL PRIMARY KEY,
        quarterly VARCHAR(20),
        promoters NUMERIC,
        fiis NUMERIC,
        diis NUMERIC,
        government NUMERIC,
        public NUMERIC,
        no_of_shareholders NUMERIC
    );
    """
    cursor.execute(create_table_query)
    conn.commit()
    cursor.close()
    conn.close()

def store_data_in_postgres(stock_symbol, data):
    if not data:
        return

    table_name = format_table_name(stock_symbol)
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    insert_query = f"""
    INSERT INTO {table_name} (
        quarterly, promoters, fiis, diis, government, public, no_of_shareholders
    ) VALUES (%s, %s, %s, %s, %s, %s, %s);
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

for stock in nse500stocklist:
    create_stock_table(stock)
    yearly_headers, stock_data = scrape_stock_data(stock)
    if stock_data:
        store_data_in_postgres(stock, stock_data)
    time.sleep(2)

print("🎯 Data scraping and database storage completed successfully!")

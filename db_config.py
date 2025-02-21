# db_config.py - Store PostgreSQL Connection Details Securely

import os

DB_CONFIG = {
    "dbname": os.getenv("PG_DBNAME", "ATTS_EquityData_NSE500"),
    "user": os.getenv("PG_USER", "postgres"),
    "password": os.getenv("PG_PASSWORD", "Alpha12"),
    "host": os.getenv("PG_HOST", "localhost"),
    "port": os.getenv("PG_PORT", "5432"),
}

# Check if running locally or in production (AWS)
USE_SSL = os.getenv("USE_SSL", "false").lower() == "true"  # Set "true" in AWS

if USE_SSL:
    DB_CONFIG.update({
        "sslmode": "require",
        "sslrootcert": "C:/Program Files/PostgreSQL/17/ssl/certs/ca-bundle.crt"  # Update when using AWS
    })
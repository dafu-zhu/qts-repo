"""
Test script to explore futures contract tables
"""
import os
from dotenv import load_dotenv
import wrds
import pandas as pd

load_dotenv()

WRDS_USERNAME = os.getenv("WRDS_USERNAME")
WRDS_PASSWORD = os.getenv("WRDS_PASSWORD")

print("Connecting to WRDS...")
db = wrds.Connection(wrds_username=WRDS_USERNAME, wrds_password=WRDS_PASSWORD)
print("Connected successfully!")

# Explore key futures tables
tables_to_check = [
    ('tr_ds_fut', 'wrds_fut_contract'),
    ('tr_ds_fut', 'wrds_fut_series'),
    ('tr_ds_fut', 'dsfutcontrval'),
    ('tr_ds_fut', 'dsfutcontrinfo'),
    ('tr_ds_fut', 'dsfutcode'),
]

for schema, table in tables_to_check:
    print("\n" + "="*80)
    print(f"Table: {schema}.{table}")
    print("="*80)
    try:
        query = f"SELECT * FROM {schema}.{table} LIMIT 2"
        result = db.raw_sql(query)
        print(f"Columns: {list(result.columns)}")
        print(f"\nFirst row:")
        print(result.head(1).to_string())
    except Exception as e:
        print(f"Error: {e}")

# Now try to find CL (Crude Oil) futures specifically
print("\n" + "="*80)
print("Searching for CL (Crude Oil) futures...")
print("="*80)

try:
    # First, find CL in the code or description table
    query = """
    SELECT *
    FROM tr_ds_fut.dsfutcode
    WHERE infocode LIKE '%CL%' OR name LIKE '%Crude%' OR name LIKE '%crude%'
    LIMIT 10
    """
    result = db.raw_sql(query)
    print(f"Found {len(result)} matches in dsfutcode:")
    print(result[['infocode', 'name']].to_string())
except Exception as e:
    print(f"Error searching codes: {e}")

# Try searching contract info
try:
    query = """
    SELECT *
    FROM tr_ds_fut.dsfutcontrinfo
    WHERE code LIKE '%CL%'
    LIMIT 10
    """
    result = db.raw_sql(query)
    print(f"\n\nFound {len(result)} matches in dsfutcontrinfo:")
    if len(result) > 0:
        print("Columns:", list(result.columns))
        print(result.to_string())
except Exception as e:
    print(f"Error searching contract info: {e}")

# Try searching WRDS tables
try:
    query = """
    SELECT *
    FROM tr_ds_fut.wrds_fut_series
    WHERE code LIKE '%CL%' OR name LIKE '%Crude%'
    LIMIT 10
    """
    result = db.raw_sql(query)
    print(f"\n\nFound {len(result)} matches in wrds_fut_series:")
    if len(result) > 0:
        print(result.to_string())
except Exception as e:
    print(f"Error searching wrds_fut_series: {e}")

# Check for specific futures codes
print("\n" + "="*80)
print("Checking for specific futures: CL, HO, YM, RTY...")
print("="*80)

for ticker in ['CL', 'HO', 'YM', 'RTY']:
    try:
        query = f"""
        SELECT DISTINCT code, name
        FROM tr_ds_fut.dsfutcode
        WHERE infocode LIKE '%{ticker}%'
        LIMIT 5
        """
        result = db.raw_sql(query)
        print(f"\n{ticker}: Found {len(result)} matches")
        if len(result) > 0:
            print(result.to_string())
    except Exception as e:
        print(f"{ticker}: Error - {e}")

db.close()
print("\nDone!")

"""
Find the mapping between ticker symbols and futcode
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

# Check dsfutdesc table - likely has descriptions
print("\n" + "="*80)
print("Checking dsfutdesc table...")
print("="*80)
try:
    query = "SELECT * FROM tr_ds_fut.dsfutdesc LIMIT 10"
    result = db.raw_sql(query)
    print("Columns:", list(result.columns))
    print("\nSample rows:")
    print(result.to_string())
except Exception as e:
    print(f"Error: {e}")

# Check dsfutclass table
print("\n" + "="*80)
print("Checking dsfutclass table...")
print("="*80)
try:
    query = "SELECT * FROM tr_ds_fut.dsfutclass LIMIT 10"
    result = db.raw_sql(query)
    print("Columns:", list(result.columns))
    print("\nSample rows:")
    print(result.to_string())
except Exception as e:
    print(f"Error: {e}")

# Check dsfutcontr table - should have contract details
print("\n" + "="*80)
print("Checking dsfutcontr table...")
print("="*80)
try:
    query = "SELECT * FROM tr_ds_fut.dsfutcontr LIMIT 10"
    result = db.raw_sql(query)
    print("Columns:", list(result.columns))
    print("\nSample rows:")
    print(result.to_string())
except Exception as e:
    print(f"Error: {e}")

# Search for Crude in description table
print("\n" + "="*80)
print("Searching for Crude Oil, Heating Oil, etc...")
print("="*80)
try:
    query = """
    SELECT *
    FROM tr_ds_fut.dsfutdesc
    WHERE name LIKE '%Crude%' OR name LIKE '%Heating%' OR name LIKE '%Russell%' OR name LIKE '%Dow%'
    """
    result = db.raw_sql(query)
    print(f"Found {len(result)} matches:")
    print(result.to_string())
except Exception as e:
    print(f"Error: {e}")

# Let's try to see dsfutcontrinfo with specific date range
print("\n" + "="*80)
print("Checking contracts around our date range (Dec 2025)...")
print("="*80)
try:
    query = """
    SELECT DISTINCT contrcode, clscode, dsmnem, expirationdate
    FROM tr_ds_fut.dsfutcontrinfo
    WHERE expirationdate >= '2025-12-01' AND expirationdate <= '2025-12-31'
    LIMIT 50
    """
    result = db.raw_sql(query)
    print(f"Found {len(result)} contracts expiring in Dec 2025:")
    print(result.to_string())
except Exception as e:
    print(f"Error: {e}")

# Try the wrds curated tables
print("\n" + "="*80)
print("Checking WRDS curated info tables...")
print("="*80)
try:
    query = "SELECT * FROM tr_ds_fut.wrds_contract_info LIMIT 10"
    result = db.raw_sql(query)
    print("Columns:", list(result.columns))
    print("\nSample rows:")
    print(result.to_string())
except Exception as e:
    print(f"Error: {e}")

try:
    query = "SELECT * FROM tr_ds_fut.wrds_cseries_info LIMIT 10"
    result = db.raw_sql(query)
    print("\n\nwrds_cseries_info:")
    print("Columns:", list(result.columns))
    print("\nSample rows:")
    print(result.to_string())
except Exception as e:
    print(f"Error: {e}")

db.close()
print("\nDone!")

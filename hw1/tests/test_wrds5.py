"""
Search for specific futures: CL, HO, YM, RTY
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

# Search for CL, HO, YM, RTY in contract names
print("\n" + "="*80)
print("Searching for Crude Oil (CL), Heating Oil (HO), YM, RTY...")
print("="*80)

search_terms = [
    ('CL', '%CRUDE%'),
    ('HO', '%HEATING%'),
    ('YM', '%DOW%MINI%'),
    ('YM', '%E-MINI%DOW%'),
    ('RTY', '%RUSSELL%2000%'),
    ('RTY', '%E-MINI%RUSSELL%'),
]

for ticker, pattern in search_terms:
    try:
        query = f"""
        SELECT DISTINCT contrcode, contrname, exchtickersymb
        FROM tr_ds_fut.dsfutcontr
        WHERE UPPER(contrname) LIKE '{pattern}'
        LIMIT 10
        """
        result = db.raw_sql(query)
        if len(result) > 0:
            print(f"\n{ticker} matches:")
            print(result.to_string())
    except Exception as e:
        print(f"{ticker}: Error - {e}")

# Check what the latest available dates are in the data
print("\n" + "="*80)
print("Checking latest available dates in futures data...")
print("="*80)

try:
    query = """
    SELECT MAX(date_) as max_date, MIN(date_) as min_date
    FROM tr_ds_fut.wrds_fut_contract
    """
    result = db.raw_sql(query)
    print("Date range in wrds_fut_contract:")
    print(result.to_string())
except Exception as e:
    print(f"Error: {e}")

# Let's search for contracts by dsmnem pattern
print("\n" + "="*80)
print("Searching by common mnemonic patterns...")
print("="*80)

# CL futures typically have dsmnem like "CL" followed by month code
for ticker in ['CL', 'HO', 'YM', 'RTY']:
    try:
        query = f"""
        SELECT contrcode, contrname, exchtickersymb
        FROM tr_ds_fut.dsfutcontr
        WHERE dscont rid LIKE '{ticker}%'
        LIMIT 5
        """
        result = db.raw_sql(query)
        if len(result) > 0:
            print(f"\n{ticker}:")
            print(result.to_string())
    except Exception as e:
        # Expected to fail, just trying
        pass

# Try searching in wrds_contract_info with exchtickersymb
print("\n" + "="*80)
print("Searching by exchange ticker symbol...")
print("="*80)

for ticker in ['CL', 'HO', 'YM', 'RTY']:
    try:
        query = f"""
        SELECT DISTINCT contrcode, contrname, exchtickersymb, dsmnem
        FROM tr_ds_fut.wrds_contract_info
        WHERE exchtickersymb = '{ticker}'
        OR dsmnem LIKE '{ticker}%'
        LIMIT 10
        """
        result = db.raw_sql(query)
        if len(result) > 0:
            print(f"\n{ticker}:")
            print(result.to_string())
    except Exception as e:
        print(f"{ticker}: Error - {e}")

# Get sample of recent contracts with dates
print("\n" + "="*80)
print("Sample of recent futures contracts...")
print("="*80)
try:
    query = """
    SELECT futcode, contrcode, contrname, exchtickersymb, startdate, lasttrddate, expirationdate
    FROM tr_ds_fut.wrds_contract_info
    WHERE startdate >= '2024-01-01'
    LIMIT 20
    """
    result = db.raw_sql(query)
    print(result.to_string())
except Exception as e:
    print(f"Error: {e}")

db.close()
print("\nDone!")

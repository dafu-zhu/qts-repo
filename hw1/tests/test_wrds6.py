"""
List all available contract names to see what's in the database
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

# Get all unique contract names
print("\n" + "="*80)
print("Listing unique contract names...")
print("="*80)

try:
    query = """
    SELECT DISTINCT contrname
    FROM tr_ds_fut.dsfutcontr
    ORDER BY contrname
    """
    result = db.raw_sql(query)
    print(f"Found {len(result)} unique contracts")

    # Filter for energy and equity index contracts
    print("\n" + "="*80)
    print("Energy-related contracts:")
    print("="*80)
    energy = result[result['contrname'].str.contains('OIL|CRUDE|HEATING|GAS|ENERGY', case=False, na=False)]
    print(energy.to_string(index=False))

    print("\n" + "="*80)
    print("Index-related contracts (DOW, RUSSELL, S&P, NASDAQ):")
    print("="*80)
    indices = result[result['contrname'].str.contains('DOW|RUSSELL|S&P|NASDAQ|INDEX', case=False, na=False)]
    print(indices.head(50).to_string(index=False))

except Exception as e:
    print(f"Error: {e}")

# Check srccode (source codes) - maybe there's a CME or NYMEX code
print("\n" + "="*80)
print("Checking source codes...")
print("="*80)

try:
    query = """
    SELECT DISTINCT srccode, COUNT(*) as num_contracts
    FROM tr_ds_fut.dsfutcontr
    GROUP BY srccode
    ORDER BY num_contracts DESC
    LIMIT 20
    """
    result = db.raw_sql(query)
    print("Top source codes:")
    print(result.to_string())
except Exception as e:
    print(f"Error: {e}")

# Check if there are contracts with CME or NYMEX in the name
print("\n" + "="*80)
print("Checking for CME/NYMEX contracts...")
print("="*80)

try:
    # Get dsfutdesc for source codes
    query = """
    SELECT code, desc_
    FROM tr_ds_fut.dsfutdesc
    WHERE type_ = 0 AND code = 1
    """
    result = db.raw_sql(query)
    print("Source code descriptions:")
    print(result.to_string())
except Exception as e:
    print(f"Error: {e}")

# Try finding source codes that might be CME/NYMEX
try:
    query = """
    SELECT type_, code, desc_
    FROM tr_ds_fut.dsfutdesc
    WHERE desc_ LIKE '%CME%' OR desc_ LIKE '%NYMEX%' OR desc_ LIKE '%NASDAQ%' OR desc_ LIKE '%CHICAGO%'
    """
    result = db.raw_sql(query)
    if len(result) > 0:
        print("\nCME/NYMEX related codes:")
        print(result.to_string())
except Exception as e:
    print(f"Error: {e}")

db.close()
print("\nDone!")

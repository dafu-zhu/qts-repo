"""
Check data availability for HO, YM, RTY contract codes
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

# Check what data exists for HO, YM, RTY
for name, contrcode in [('HO', 412), ('YM', 276), ('RTY', 325)]:
    print("\n" + "="*80)
    print(f"{name} (contrcode {contrcode}) - checking all contracts...")
    print("="*80)

    try:
        query = f"""
        SELECT futcode, dsmnem, lasttrddate, expirationdate, startdate
        FROM tr_ds_fut.wrds_contract_info
        WHERE contrcode = {contrcode}
        ORDER BY lasttrddate DESC
        LIMIT 20
        """
        result = db.raw_sql(query)
        print(f"Found {len(result)} contracts (showing latest 20):")
        if len(result) > 0:
            print(result.to_string())
    except Exception as e:
        print(f"Error: {e}")

# Let's try the alternative HO code (2029.0) which shows "HEATING OIL (NEW YORK)"
print("\n" + "="*80)
print("Trying alternative HO code (NEW YORK HEATING OIL - 2029)...")
print("="*80)

try:
    query = """
    SELECT futcode, dsmnem, lasttrddate, expirationdate, startdate
    FROM tr_ds_fut.wrds_contract_info
    WHERE contrcode = 2029
    AND startdate <= '2025-12-19'
    AND lasttrddate >= '2025-12-01'
    ORDER BY lasttrddate
    LIMIT 20
    """
    result = db.raw_sql(query)
    print(f"Found {len(result)} HO contracts:")
    if len(result) > 0:
        print(result.to_string())
except Exception as e:
    print(f"Error: {e}")

# Try E-mini Russell 2000 (4396.0 - CME E-mini Russell 2000)
print("\n" + "="*80)
print("Trying CME E-mini Russell 2000 (contrcode 4396)...")
print("="*80)

try:
    query = """
    SELECT futcode, dsmnem, lasttrddate, expirationdate, startdate
    FROM tr_ds_fut.wrds_contract_info
    WHERE contrcode = 4396
    AND startdate <= '2025-12-19'
    AND lasttrddate >= '2025-12-01'
    ORDER BY lasttrddate
    LIMIT 20
    """
    result = db.raw_sql(query)
    print(f"Found {len(result)} RTY contracts:")
    if len(result) > 0:
        print(result.to_string())
except Exception as e:
    print(f"Error: {e}")

# Try Big Dow / Micro E-Mini Dow
print("\n" + "="*80)
print("Trying Micro E-Mini Dow (contrcode 4712)...")
print("="*80)

try:
    query = """
    SELECT futcode, dsmnem, lasttrddate, expirationdate, startdate
    FROM tr_ds_fut.wrds_contract_info
    WHERE contrcode = 4712
    AND startdate <= '2025-12-19'
    AND lasttrddate >= '2025-12-01'
    ORDER BY lasttrddate
    LIMIT 20
    """
    result = db.raw_sql(query)
    print(f"Found {len(result)} YM contracts:")
    if len(result) > 0:
        print(result.to_string())
except Exception as e:
    print(f"Error: {e}")

db.close()
print("\nDone!")

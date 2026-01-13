"""
Get specific contract codes for CL, HO, YM (Dow), RTY (Russell)
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

# Get contract codes for our specific futures
searches = [
    ('CL - Crude Oil', ['CRUDE OIL (LIGHT SWEET)', 'CRUDE OIL (WEST TEXAS INTERMEDIATE)']),
    ('HO - Heating Oil', ['HEATING OIL']),
    ('YM - Dow Jones', ['BIG DOW', 'DOW JONES INDUSTRIAL', 'E-MINI DOW', 'MICRO E-MINI DOW']),
    ('RTY - Russell 2000', ['RUSSELL 2000', 'E-MINI RUSSELL']),
]

all_results = {}

for label, patterns in searches:
    print("\n" + "="*80)
    print(f"Searching for: {label}")
    print("="*80)

    for pattern in patterns:
        try:
            query = f"""
            SELECT contrcode, contrname, exchtickersymb, dscontrid
            FROM tr_ds_fut.dsfutcontr
            WHERE UPPER(contrname) LIKE '%{pattern.upper()}%'
            """
            result = db.raw_sql(query)

            if len(result) > 0:
                print(f"\nFound {len(result)} contracts matching '{pattern}':")
                print(result.to_string())
                all_results[label] = result.iloc[0]['contrcode']  # Save first match
                break
        except Exception as e:
            print(f"Error searching '{pattern}': {e}")

# Now get some sample contracts and their data for December 2025
print("\n" + "="*80)
print("Getting contracts for December 2025...")
print("="*80)

for label, contrcode in all_results.items():
    try:
        query = f"""
        SELECT futcode, contrcode, contrname, dsmnem, lasttrddate, expirationdate, startdate
        FROM tr_ds_fut.wrds_contract_info
        WHERE contrcode = {contrcode}
        AND (lasttrddate >= '2025-11-01' AND lasttrddate <= '2026-02-28')
        ORDER BY lasttrddate
        LIMIT 5
        """
        result = db.raw_sql(query)

        if len(result) > 0:
            print(f"\n{label} (contrcode={contrcode}):")
            print(result.to_string())
    except Exception as e:
        print(f"Error for {label}: {e}")

# Check sample price data
print("\n" + "="*80)
print("Checking sample price data availability...")
print("="*80)

if 'CL - Crude Oil' in all_results:
    contrcode = all_results['CL - Crude Oil']
    try:
        query = f"""
        SELECT c.futcode, c.dsmnem, c.lasttrddate, COUNT(v.date_) as num_days
        FROM tr_ds_fut.wrds_contract_info c
        LEFT JOIN tr_ds_fut.wrds_fut_contract v ON c.futcode = v.futcode
        WHERE c.contrcode = {contrcode}
        AND v.date_ >= '2025-12-12' AND v.date_ <= '2025-12-19'
        GROUP BY c.futcode, c.dsmnem, c.lasttrddate
        ORDER BY c.lasttrddate
        LIMIT 10
        """
        result = db.raw_sql(query)

        print(f"\nCrude Oil contracts with data in Dec 12-19, 2025:")
        print(result.to_string())
    except Exception as e:
        print(f"Error: {e}")

db.close()
print("\nDone!")

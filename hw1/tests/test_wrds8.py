"""
Direct search for contracts
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

# Get all contracts and filter in Python
print("\n" + "="*80)
print("Loading all contract names...")
print("="*80)

try:
    query = """
    SELECT contrcode, contrname, exchtickersymb, dscontrid
    FROM tr_ds_fut.dsfutcontr
    """
    all_contracts = db.raw_sql(query)
    print(f"Loaded {len(all_contracts)} contracts")

    # Search for our specific contracts
    print("\n" + "="*80)
    print("Crude Oil contracts:")
    print("="*80)
    crude = all_contracts[all_contracts['contrname'].str.contains('CRUDE OIL', case=False, na=False)]
    print(crude[['contrcode', 'contrname', 'exchtickersymb']].to_string())

    print("\n" + "="*80)
    print("Heating Oil contracts:")
    print("="*80)
    heating = all_contracts[all_contracts['contrname'].str.contains('HEATING OIL', case=False, na=False)]
    print(heating[['contrcode', 'contrname', 'exchtickersymb']].to_string())

    print("\n" + "="*80)
    print("Dow contracts:")
    print("="*80)
    dow = all_contracts[all_contracts['contrname'].str.contains('DOW', case=False, na=False)]
    print(dow[['contrcode', 'contrname', 'exchtickersymb']].to_string())

    print("\n" + "="*80)
    print("Russell contracts:")
    print("="*80)
    russell = all_contracts[all_contracts['contrname'].str.contains('RUSSELL', case=False, na=False)]
    print(russell[['contrcode', 'contrname', 'exchtickersymb']].to_string())

    # Get specific contract codes
    cl_code = crude[crude['contrname'] == 'CRUDE OIL (LIGHT SWEET)']['contrcode'].values
    ho_code = heating[heating['contrname'] == 'HEATING OIL']['contrcode'].values
    ym_code = dow[dow['contrname'].str.contains('BIG DOW', case=False, na=False)]['contrcode'].values
    rty_code = russell[russell['contrname'].str.contains('RUSSELL 2000', case=False, na=False)]['contrcode'].values

    print("\n" + "="*80)
    print("Selected contract codes:")
    print("="*80)
    if len(cl_code) > 0:
        print(f"CL (Crude Oil Light Sweet): {cl_code[0]}")
    if len(ho_code) > 0:
        print(f"HO (Heating Oil): {ho_code[0]}")
    if len(ym_code) > 0:
        print(f"YM (Dow): {ym_code[0]}")
    if len(rty_code) > 0:
        print(f"RTY (Russell 2000): {rty_code[0]}")

    # Get contracts for December 2025
    print("\n" + "="*80)
    print("Getting contracts for Dec 2025...")
    print("="*80)

    for name, codes in [('CL', cl_code), ('HO', ho_code), ('YM', ym_code), ('RTY', rty_code)]:
        if len(codes) > 0:
            contrcode = codes[0]
            query = f"""
            SELECT futcode, dsmnem, lasttrddate, expirationdate, startdate
            FROM tr_ds_fut.wrds_contract_info
            WHERE contrcode = {contrcode}
            AND startdate <= '2025-12-19'
            AND lasttrddate >= '2025-12-01'
            ORDER BY lasttrddate
            """
            result = db.raw_sql(query)
            print(f"\n{name}:")
            print(result.to_string())

except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()

db.close()
print("\nDone!")

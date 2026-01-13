"""
Check the actual structure of downloaded data
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

# Check CL data
contrcode = 1986  # CL
query = f"""
SELECT
    c.futcode,
    c.contrcode,
    c.dsmnem,
    c.lasttrddate,
    v.date_,
    v.open_,
    v.high,
    v.low,
    v.settlement,
    v.volume,
    v.openinterest
FROM tr_ds_fut.wrds_contract_info c
INNER JOIN tr_ds_fut.wrds_fut_contract v ON c.futcode = v.futcode
WHERE c.contrcode = {contrcode}
AND v.date_ >= '2025-12-12'
AND v.date_ <= '2025-12-19'
AND c.startdate <= '2025-12-19'
AND c.lasttrddate >= '2025-12-01'
ORDER BY v.date_, c.lasttrddate
LIMIT 20
"""

df = db.raw_sql(query)
print("\n" + "="*80)
print("Sample CL data:")
print("="*80)
print(df.to_string())

# Check what the contract with most data looks like in detail
print("\n" + "="*80)
print("Checking futcode 349339 (NCL1226) - should have data...")
print("="*80)

query2 = """
SELECT date_, open_, high, low, settlement, volume, openinterest
FROM tr_ds_fut.wrds_fut_contract
WHERE futcode = 349339.0
AND date_ >= '2025-12-12'
AND date_ <= '2025-12-19'
ORDER BY date_
"""

df2 = db.raw_sql(query2)
print(df2.to_string())

# Check futcode 417987 (HO front month)
print("\n" + "="*80)
print("Checking futcode 417987 (NHO0126) - HO front month...")
print("="*80)

query3 = """
SELECT date_, open_, high, low, settlement, volume, openinterest
FROM tr_ds_fut.wrds_fut_contract
WHERE futcode = 417987
AND date_ >= '2025-12-12'
AND date_ <= '2025-12-19'
ORDER BY date_
"""

df3 = db.raw_sql(query3)
print(df3.to_string())

db.close()
print("\nDone!")

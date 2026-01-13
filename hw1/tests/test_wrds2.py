"""
Test script to explore Thomson Reuters futures data
"""
import os
from dotenv import load_dotenv
import wrds

load_dotenv()

WRDS_USERNAME = os.getenv("WRDS_USERNAME")
WRDS_PASSWORD = os.getenv("WRDS_PASSWORD")

print("Connecting to WRDS...")
db = wrds.Connection(wrds_username=WRDS_USERNAME, wrds_password=WRDS_PASSWORD)
print("Connected successfully!")

# Check Thomson Reuters futures libraries
print("\n" + "="*80)
print("Checking Thomson Reuters futures libraries...")
print("="*80)

futures_libs = ['tr_ds_fut', 'trsamp_dsfut', 'trdstrm']

for lib in futures_libs:
    print(f"\n{'-'*80}")
    print(f"Library: {lib}")
    print(f"{'-'*80}")
    try:
        tables = db.list_tables(library=lib)
        print(f"Found {len(tables)} tables:")
        for table in tables:
            print(f"  - {table}")
    except Exception as e:
        print(f"Error: {e}")

# Try to query for CL futures specifically
print("\n" + "="*80)
print("Trying to query CL (Crude Oil) futures data...")
print("="*80)

# Try different table names
test_queries = [
    ("tr_ds_fut", "futures"),
    ("tr_ds_fut", "daily"),
    ("trsamp_dsfut", "futures"),
    ("trsamp_dsfut", "fut"),
]

for schema, table in test_queries:
    try:
        query = f"""
        SELECT *
        FROM {schema}.{table}
        WHERE ticker LIKE '%CL%' OR symbol LIKE '%CL%' OR code LIKE '%CL%'
        LIMIT 5
        """
        print(f"\nTrying {schema}.{table}...")
        result = db.raw_sql(query)
        if len(result) > 0:
            print(f"✓ Found data in {schema}.{table}")
            print("Columns:", result.columns.tolist())
            print("\nSample data:")
            print(result.head())
            break
        else:
            print(f"  No CL data found in {schema}.{table}")
    except Exception as e:
        print(f"  ✗ Error with {schema}.{table}: {str(e)[:150]}")

# Let's also try to describe the structure of any futures table we can find
print("\n" + "="*80)
print("Exploring table structure...")
print("="*80)

for schema, table in test_queries:
    try:
        query = f"""
        SELECT *
        FROM {schema}.{table}
        LIMIT 1
        """
        result = db.raw_sql(query)
        print(f"\n✓ Table {schema}.{table} structure:")
        print("Columns:", result.columns.tolist())
        break
    except:
        continue

db.close()
print("\nDone!")

"""
Test script to explore WRDS database structure
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

# List all libraries
print("\n" + "="*80)
print("Listing all available libraries...")
print("="*80)
libs = db.list_libraries()
print(libs)

# Search for CME or futures related schemas
print("\n" + "="*80)
print("Searching for CME/futures related schemas...")
print("="*80)
cme_libs = [lib for lib in libs if 'cme' in lib.lower() or 'future' in lib.lower() or 'nymex' in lib.lower()]
print(f"Found {len(cme_libs)} relevant libraries:")
for lib in cme_libs:
    print(f"  - {lib}")

# List tables in each relevant schema
for lib in cme_libs[:5]:  # Limit to first 5 to avoid too much output
    print(f"\n" + "="*80)
    print(f"Tables in '{lib}':")
    print("="*80)
    try:
        tables = db.list_tables(library=lib)
        print(f"Found {len(tables)} tables:")
        for table in tables[:20]:  # Show first 20 tables
            print(f"  - {table}")
    except Exception as e:
        print(f"Error listing tables: {e}")

# Try to describe a specific table structure if it exists
print("\n" + "="*80)
print("Trying to find CME futures data...")
print("="*80)

# Common table names to try
possible_tables = [
    ('cme', 'futures'),
    ('cme', 'future'),
    ('cme_tfm', 'futures'),
    ('cme_tfm', 'daily'),
    ('tfm', 'futures'),
    ('tfm', 'daily'),
]

for schema, table in possible_tables:
    try:
        query = f"""
        SELECT *
        FROM {schema}.{table}
        LIMIT 1
        """
        result = db.raw_sql(query)
        print(f"\n✓ Found table: {schema}.{table}")
        print("Columns:", result.columns.tolist())
        print("Sample row:")
        print(result.head(1))
        break
    except Exception as e:
        print(f"✗ {schema}.{table} - {str(e)[:100]}")

db.close()
print("\nDone!")

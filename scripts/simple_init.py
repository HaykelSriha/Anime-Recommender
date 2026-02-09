"""Simple warehouse initialization - no fancy logging"""
import duckdb
from pathlib import Path

BASE_DIR = Path(__file__).parent.parent
WAREHOUSE_DB = BASE_DIR / 'warehouse' / 'anime_dw.duckdb'
DDL_DIR = BASE_DIR / 'warehouse' / 'schema' / 'ddl'

# Create database
conn = duckdb.connect(str(WAREHOUSE_DB))
print(f"Connected to {WAREHOUSE_DB}")

# Execute DDL files
ddl_files = [
    '01_create_dimensions.sql',
    '02_create_facts.sql',
    '03_create_bridge_tables.sql',
    '04_create_metadata_tables.sql',
    '05_create_views.sql',
]

for ddl_file in ddl_files:
    file_path = DDL_DIR / ddl_file
    print(f"\nExecuting {ddl_file}...")

    with open(file_path, 'r', encoding='utf-8') as f:
        sql = f.read()

    try:
        conn.executemany(sql)
        print(f"OK - {ddl_file}")
    except Exception as e:
        print(f"ERROR in {ddl_file}: {e}")

# Verify
print("\n=== VERIFICATION ===")
tables = conn.execute("SHOW TABLES").fetchall()
print(f"Tables created: {len(tables)}")
for table in tables[:10]:
    print(f"  - {table[0]}")

conn.close()
print("\nDone!")

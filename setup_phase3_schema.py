"""Setup Phase 3 schema - create model storage tables"""
import duckdb

warehouse = "warehouse/anime_full_phase1.duckdb"

print("Setting up Phase 3 schema...")

try:
    conn = duckdb.connect(warehouse)

    # Read and execute DDL
    with open("warehouse/schema/ddl/08_create_model_tables.sql", "r") as f:
        ddl = f.read()

    # Split by statements and execute each
    for statement in ddl.split(";"):
        if statement.strip():
            try:
                conn.execute(statement)
                print(f"[OK] {statement.split('CREATE')[1].split('(')[0].strip() if 'CREATE' in statement else 'Statement'}")
            except Exception as e:
                # Table might already exist, that's ok
                if "already exists" not in str(e).lower():
                    print(f"[WARN] {str(e)[:100]}")

    conn.commit()
    conn.close()

    print("\n[OK] Phase 3 schema setup complete")

except Exception as e:
    print(f"ERROR: {e}")
    import traceback
    traceback.print_exc()

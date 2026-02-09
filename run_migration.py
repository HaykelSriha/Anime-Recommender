import duckdb

# Connect to the warehouse
conn = duckdb.connect('warehouse/anime_dw.duckdb')

print("Running migration: 002_add_anime_relations...")

try:
    # Add columns to dim_anime
    print("  - Adding relations column...")
    conn.execute("ALTER TABLE dim_anime ADD COLUMN IF NOT EXISTS relations TEXT")

    print("  - Adding parent_anime_id column...")
    conn.execute("ALTER TABLE dim_anime ADD COLUMN IF NOT EXISTS parent_anime_id INTEGER")

    print("  - Adding series_root_id column...")
    conn.execute("ALTER TABLE dim_anime ADD COLUMN IF NOT EXISTS series_root_id INTEGER")

    # Create bridge table
    print("  - Creating bridge_anime_relations table...")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS bridge_anime_relations (
            source_anime_id INTEGER NOT NULL,
            target_anime_id INTEGER NOT NULL,
            relation_type VARCHAR(50),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (source_anime_id, target_anime_id)
        )
    """)

    # Create indexes
    print("  - Creating indexes...")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_dim_anime_parent_id ON dim_anime(parent_anime_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_dim_anime_series_root_id ON dim_anime(series_root_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_bridge_relations_source ON bridge_anime_relations(source_anime_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_bridge_relations_target ON bridge_anime_relations(target_anime_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_bridge_relations_type ON bridge_anime_relations(relation_type)")

    conn.close()
    print("\n✅ Migration completed successfully!")

except Exception as e:
    print(f"\n❌ Migration failed: {str(e)}")
    conn.close()

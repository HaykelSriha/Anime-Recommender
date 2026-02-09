import duckdb

conn = duckdb.connect('warehouse/anime_dw.duckdb')

print("Checking relation data in warehouse...\n")

# Check if bridge table has any relations
count = conn.execute("SELECT COUNT(*) FROM bridge_anime_relations").fetchone()[0]
print(f"Total relations in bridge table: {count}")

# Check if a specific anime has relations
aot_relations = conn.execute("""
    SELECT target_anime_id, relation_type
    FROM bridge_anime_relations
    WHERE source_anime_id = 16498
    LIMIT 10
""").fetchall()

print(f"\nAttack on Titan (ID 16498) relations found: {len(aot_relations)}")
for target_id, rel_type in aot_relations:
    print(f"  - Target: {target_id}, Type: {rel_type}")

# Check if the relations column is populated in dim_anime
relations_count = conn.execute("""
    SELECT COUNT(*) FROM dim_anime WHERE relations IS NOT NULL
""").fetchone()[0]
print(f"\nAnime with relations data in dim_anime: {relations_count}")

# Show a sample
sample = conn.execute("""
    SELECT anime_id, title, relations FROM dim_anime
    WHERE relations IS NOT NULL LIMIT 1
""").fetchall()

if sample:
    print(f"\nSample anime with relations:")
    for anime_id, title, relations_str in sample:
        print(f"  ID: {anime_id}, Title: {title}")
        print(f"  Relations: {relations_str[:200]}...")

conn.close()
print("\n✓ Check complete")

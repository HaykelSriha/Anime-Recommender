"""Phase 2: Ultra-fast rating generation using DuckDB native operations"""
import duckdb

warehouse = "warehouse/anime_full_phase1.duckdb"

print("\n[ULTRA-FAST] PHASE 2: SQL-NATIVE RATING GENERATION")
print("=" * 80)

try:
    conn = duckdb.connect(warehouse)

    print("\nGenerating synthetic ratings using SQL...")

    # Get current max rating_key
    max_key = conn.execute("SELECT COALESCE(MAX(rating_key), 0) FROM fact_user_rating").fetchone()[0]
    print(f"Starting from rating_key: {max_key + 1}")

    # Create ratings using pure SQL - use row_number to generate sequential keys
    sql = f"""
    INSERT INTO fact_user_rating (rating_key, user_key, anime_key, rating, reviewed_date, rating_source)
    WITH user_anime_pairs AS (
        SELECT
            u.user_key,
            a.anime_key,
            -- Use hash to deterministically select ~8% of pairs
            abs(hash(u.user_key || '_' || a.anime_key)) % 100 as hash_val
        FROM dim_user u
        CROSS JOIN dim_anime a
        WHERE u.user_key <= 1000  -- Start with 1000 users for speed
    ),
    selected_pairs AS (
        SELECT
            user_key,
            anime_key,
            CASE
                WHEN hash_val < 5 THEN 1
                WHEN hash_val < 13 THEN 2
                WHEN hash_val < 30 THEN 3
                WHEN hash_val < 65 THEN 4
                ELSE 5
            END as rating,
            CURRENT_TIMESTAMP - INTERVAL (random() * 730) DAY as reviewed_date
        FROM user_anime_pairs
        WHERE hash_val < 8  -- ~8% of pairs get rated
    )
    SELECT
        row_number() OVER (ORDER BY user_key, anime_key) + {max_key} as rating_key,
        user_key,
        anime_key,
        rating,
        reviewed_date,
        'synthetic'
    FROM selected_pairs
    """

    conn.execute(sql)
    conn.commit()

    # Check results
    total = conn.execute("SELECT COUNT(*) FROM fact_user_rating").fetchone()[0]
    users_with_ratings = conn.execute("SELECT COUNT(DISTINCT user_key) FROM fact_user_rating").fetchone()[0]

    print(f"\n[SUCCESS] RATINGS GENERATED")
    print(f"  Total ratings: {total:,}")
    print(f"  Users with ratings: {users_with_ratings:,}")
    print(f"  Avg ratings per user: {total // max(users_with_ratings, 1)}")

    # Show distribution
    dist = conn.execute("""
        SELECT rating, COUNT(*) as count,
               ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM fact_user_rating), 1) as pct
        FROM fact_user_rating
        GROUP BY rating
        ORDER BY rating DESC
    """).fetchall()

    print(f"\n  Rating distribution:")
    for rating, count, pct in dist:
        print(f"    {int(rating)} stars: {count:7,} ({pct:5.1f}%)")

    conn.close()

except Exception as e:
    print(f"ERROR: {e}")
    import traceback
    traceback.print_exc()

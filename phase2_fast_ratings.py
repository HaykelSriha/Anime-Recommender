"""Phase 2: Fast batch rating generation using DuckDB"""
import duckdb
import random
import numpy as np
from datetime import datetime, timedelta

warehouse = "warehouse/anime_full_phase1.duckdb"

print("\n[FAST] PHASE 2: USER RATINGS BATCH GENERATION")
print("=" * 80)

try:
    conn = duckdb.connect(warehouse)

    # Get counts
    total_anime = conn.execute("SELECT COUNT(*) FROM dim_anime").fetchone()[0]
    total_users = conn.execute("SELECT COUNT(*) FROM dim_user").fetchone()[0]

    print(f"\nGenerating ratings for {total_users} users...")

    # Generate all ratings at once using numpy (much faster)
    all_ratings = []
    rating_key = 1  # Auto-increment counter

    for user_key in range(1, min(total_users + 1, 2001)):  # Start with 2000 users for speed
        # Random number of anime each user rates (80-120)
        num_anime_to_rate = random.randint(80, 120)
        anime_keys = random.sample(range(1, total_anime + 1), min(num_anime_to_rate, total_anime))

        for anime_key in anime_keys:
            # Generate rating with realistic distribution
            rand = random.random()
            if rand < 0.05:
                rating = 1
            elif rand < 0.13:
                rating = 2
            elif rand < 0.30:
                rating = 3
            elif rand < 0.65:
                rating = 4
            else:
                rating = 5

            # Random review date
            days_ago = random.randint(0, 730)
            review_date = (datetime.now() - timedelta(days=days_ago)).strftime('%Y-%m-%d')

            all_ratings.append((rating_key, user_key, anime_key, rating, review_date, 'synthetic'))
            rating_key += 1

        if (user_key % 250) == 0:
            print(f"  Generated ratings for {user_key} users ({len(all_ratings)} ratings)...")

    print(f"\nInserting {len(all_ratings)} ratings into database (batch mode)...")

    # Bulk insert using DuckDB's fast path (with explicit rating_key)
    for i in range(0, len(all_ratings), 1000):
        batch = all_ratings[i:i+1000]
        conn.executemany(
            "INSERT INTO fact_user_rating (rating_key, user_key, anime_key, rating, reviewed_date, rating_source) VALUES (?, ?, ?, ?, CAST(? as TIMESTAMP), ?)",
            batch
        )
        if (i % 5000) == 0:
            print(f"  Inserted {i}/{len(all_ratings)} ratings...")

    conn.commit()

    # Verify
    final_count = conn.execute("SELECT COUNT(*) FROM fact_user_rating").fetchone()[0]

    print(f"\n[SUCCESS] RATINGS LOADED")
    print(f"  Total ratings: {final_count}")
    print(f"  Avg per user rated: {final_count / 2000:.0f}")

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
        stars = "★" * int(rating) + "☆" * (5-int(rating))
        print(f"    {int(rating)} stars ({stars}): {count:6} ({pct:5.1f}%)")

    conn.close()

except Exception as e:
    print(f"ERROR: {e}")
    import traceback
    traceback.print_exc()

"""
Warehouse Inspector
===================
Quick tool to inspect the data warehouse contents
"""

import duckdb
import pandas as pd
from pathlib import Path

# Connect to warehouse
db_path = Path(__file__).parent.parent / 'warehouse' / 'anime_dw.duckdb'
conn = duckdb.connect(str(db_path))

print("=" * 80)
print("ANIME DATA WAREHOUSE INSPECTION")
print("=" * 80)

# 1. List all tables and views
print("\n📋 TABLES AND VIEWS:")
tables = conn.execute("SHOW TABLES").fetchall()
for table in tables:
    row_count = conn.execute(f"SELECT COUNT(*) FROM {table[0]}").fetchone()[0]
    print(f"  • {table[0]:35s} {row_count:6d} rows")

# 2. Warehouse statistics
print("\n📊 WAREHOUSE STATISTICS:")
stats = conn.execute("""
    SELECT
        COUNT(*) as total_anime,
        AVG(averageScore) as avg_score,
        MAX(averageScore) as max_score,
        AVG(popularity) as avg_popularity,
        MAX(popularity) as max_popularity,
        COUNT(DISTINCT format) as formats,
        MAX(snapshot_date) as last_updated
    FROM vw_anime_current
""").fetchone()

print(f"  • Total Anime: {stats[0]}")
print(f"  • Average Score: {stats[1]:.2f}/100")
print(f"  • Highest Score: {stats[2]}/100")
print(f"  • Average Popularity: {stats[3]:,.0f}")
print(f"  • Most Popular: {stats[4]:,.0f}")
print(f"  • Format Types: {stats[5]}")
print(f"  • Last Updated: {stats[6]}")

# 3. Genre breakdown
print("\n🎭 GENRE BREAKDOWN:")
genres = conn.execute("""
    SELECT g.genre_name, COUNT(DISTINCT bg.anime_key) as anime_count
    FROM dim_genre g
    LEFT JOIN bridge_anime_genre bg ON g.genre_key = bg.genre_key
    GROUP BY g.genre_name
    ORDER BY anime_count DESC
    LIMIT 10
""").fetchall()

for genre, count in genres:
    bar = '█' * (count // 2)
    print(f"  {genre:20s} {bar} {count}")

# 4. Top 10 anime by score
print("\n⭐ TOP 10 ANIME BY SCORE:")
top_anime = conn.execute("""
    SELECT title, averageScore, popularity, genres
    FROM vw_anime_current
    WHERE averageScore IS NOT NULL
    ORDER BY averageScore DESC, popularity DESC
    LIMIT 10
""").fetchall()

for i, (title, score, pop, genres) in enumerate(top_anime, 1):
    genres_short = genres[:30] + '...' if genres and len(genres) > 30 else genres or ''
    print(f"  {i:2d}. {title[:40]:40s} {score:5.1f} ({pop:8,d} pop) | {genres_short}")

# 5. Similarity scores available
print("\n🔗 RECOMMENDATION SYSTEM:")
sim_count = conn.execute("SELECT COUNT(*) FROM fact_anime_similarity").fetchone()[0]
anime_with_recs = conn.execute("SELECT COUNT(DISTINCT anime_key_1) FROM fact_anime_similarity").fetchone()[0]
print(f"  • Similarity scores stored: {sim_count}")
print(f"  • Anime with recommendations: {anime_with_recs}")

# 6. Sample recommendation
if anime_with_recs > 0:
    sample = conn.execute("""
        SELECT a1.title as source, a2.title as recommendation, s.similarity_score
        FROM fact_anime_similarity s
        JOIN vw_anime_current a1 ON s.anime_key_1 = a1.anime_key
        JOIN vw_anime_current a2 ON s.anime_key_2 = a2.anime_key
        ORDER BY s.similarity_score DESC
        LIMIT 3
    """).fetchall()

    print("\n  Sample recommendations:")
    for source, rec, score in sample:
        print(f"    {source[:30]:30s} → {rec[:30]:30s} (score: {score:.4f})")

# 7. Data freshness
print("\n📅 DATA FRESHNESS:")
metrics_dates = conn.execute("""
    SELECT
        MIN(snapshot_date) as oldest,
        MAX(snapshot_date) as newest,
        COUNT(DISTINCT snapshot_date) as snapshots
    FROM fact_anime_metrics
""").fetchone()

print(f"  • Oldest snapshot: {metrics_dates[0]}")
print(f"  • Newest snapshot: {metrics_dates[1]}")
print(f"  • Total snapshots: {metrics_dates[2]}")

print("\n" + "=" * 80)

conn.close()

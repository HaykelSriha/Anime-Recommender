"""Quick CSV migration without complex logging"""
import pandas as pd
import duckdb
from pathlib import Path
from datetime import date

BASE_DIR = Path(__file__).parent.parent
DB = BASE_DIR / 'warehouse' / 'anime_dw.duckdb'
CSV = BASE_DIR / 'data' / 'clean' / 'anime_clean.csv'

# Load CSV
print(f"Loading {CSV}...")
df = pd.read_csv(CSV)
print(f"Loaded {len(df)} records")

# Connect
conn = duckdb.connect(str(DB))
print(f"Connected to {DB}")

# Insert anime dimensions
print("\nInserting anime...")
anime_keys = {}
for idx, row in df.iterrows():
    anime_key = idx + 1
    conn.execute("""
        INSERT INTO dim_anime (anime_key, anime_id, title, description, site_url, cover_image_url)
        VALUES (?, ?, ?, ?, ?, ?)
    """, [anime_key, int(row['id']), str(row['title']), str(row['description']) if pd.notna(row['description']) else None,
          str(row['siteUrl']) if pd.notna(row['siteUrl']) else None, str(row['coverImage']) if pd.notna(row['coverImage']) else None])
    anime_keys[int(row['id'])] = anime_key
print(f"Inserted {len(anime_keys)} anime")

# Get format keys
format_keys = {name: key for key, name in conn.execute("SELECT format_key, format_name FROM dim_format").fetchall()}

# Insert metrics
print("\nInserting metrics...")
for idx, row in df.iterrows():
    anime_key = anime_keys[int(row['id'])]
    format_key = format_keys.get(str(row['format'])) if pd.notna(row['format']) else None

    conn.execute("""
        INSERT INTO fact_anime_metrics (metric_key, anime_key, format_key, average_score, popularity, episodes, snapshot_date)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, [idx + 1, anime_key, format_key,
          float(row['averageScore']) if pd.notna(row['averageScore']) else None,
          int(row['popularity']) if pd.notna(row['popularity']) else None,
          int(row['episodes']) if pd.notna(row['episodes']) else None,
          date.today()])
print(f"Inserted {len(df)} metrics")

# Get genre keys
genre_keys = {name: key for key, name in conn.execute("SELECT genre_key, genre_name FROM dim_genre").fetchall()}

# Insert genres
print("\nInserting genre relationships...")
count = 0
for _, row in df.iterrows():
    anime_key = anime_keys[int(row['id'])]
    if pd.notna(row['genres']):
        genres = [g.strip() for g in str(row['genres']).split('|')]
        for genre_name in genres:
            if genre_name in genre_keys:
                conn.execute("INSERT INTO bridge_anime_genre (anime_key, genre_key) VALUES (?, ?)",
                            [anime_key, genre_keys[genre_name]])
                count += 1
print(f"Inserted {count} relationships")

# Verify
print("\n=== VERIFICATION ===")
anime_count = conn.execute("SELECT COUNT(*) FROM vw_anime_current").fetchone()[0]
print(f"vw_anime_current: {anime_count} records")

if anime_count > 0:
    print("\nSample data:")
    for row in conn.execute("SELECT title, averageScore, popularity, genres FROM vw_anime_current LIMIT 3").fetchall():
        print(f"  {row[0][:30]:30s} | Score: {row[1]:5.1f} | Pop: {row[2]:7d} | Genres: {row[3][:30]}")

conn.close()
print("\nDone!")

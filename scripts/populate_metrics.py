"""Populate fact_anime_metrics from AniList API for all warehouse anime"""
import duckdb
import requests
import time
from datetime import date

ANILIST_URL = "https://graphql.anilist.co"

QUERY = """
query ($ids: [Int]) {
    Page(perPage: 50) {
        media(id_in: $ids, type: ANIME) {
            id
            averageScore
            popularity
            episodes
            duration
            favourites
            format
        }
    }
}
"""

FORMAT_MAP = {
    "TV": 1,
    "MOVIE": 2,
    "OVA": 3,
    "ONA": 4,
    "SPECIAL": 5,
    "MUSIC": 6,
}


def fetch_batch(anime_ids):
    """Fetch metrics for a batch of anime IDs from AniList"""
    response = requests.post(
        ANILIST_URL,
        json={"query": QUERY, "variables": {"ids": anime_ids}},
        headers={"Content-Type": "application/json"},
    )
    if response.status_code == 429:
        retry_after = int(response.headers.get("Retry-After", 60))
        print(f"  Rate limited, waiting {retry_after}s...")
        time.sleep(retry_after)
        return fetch_batch(anime_ids)

    response.raise_for_status()
    data = response.json()
    return data["data"]["Page"]["media"]


def main():
    db_path = "warehouse/anime_full_phase1.duckdb"
    conn = duckdb.connect(db_path)

    # Get all anime_ids and their anime_keys
    anime = conn.execute(
        "SELECT anime_key, anime_id FROM dim_anime WHERE is_current = true"
    ).fetchall()
    anime_map = {row[1]: row[0] for row in anime}  # anime_id -> anime_key
    all_ids = list(anime_map.keys())

    print(f"Fetching metrics for {len(all_ids)} anime from AniList...")

    # Batch fetch (50 per request, ~24 requests)
    snapshot_date = date.today()
    metric_key = 1
    total_loaded = 0

    for i in range(0, len(all_ids), 50):
        batch_ids = all_ids[i : i + 50]
        batch_num = i // 50 + 1
        total_batches = (len(all_ids) + 49) // 50

        try:
            results = fetch_batch(batch_ids)
        except Exception as e:
            print(f"  Batch {batch_num} failed: {e}")
            continue

        for media in results:
            anime_id = media["id"]
            anime_key = anime_map.get(anime_id)
            if anime_key is None:
                continue

            avg_score = media.get("averageScore")
            popularity = media.get("popularity")
            episodes = media.get("episodes")
            duration = media.get("duration")
            favourites = media.get("favourites")
            fmt = media.get("format")
            format_key = FORMAT_MAP.get(fmt)

            conn.execute(
                """INSERT INTO fact_anime_metrics
                   (metric_key, anime_key, format_key, average_score, popularity,
                    episodes, duration_minutes, favorites, snapshot_date, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)""",
                [metric_key, anime_key, format_key, avg_score, popularity,
                 episodes, duration, favourites, snapshot_date],
            )
            metric_key += 1
            total_loaded += 1

        print(f"  Batch {batch_num}/{total_batches}: fetched {len(results)} anime")

        # Rate limit: ~1 second between requests
        time.sleep(1.2)

    conn.commit()

    # Verify
    count = conn.execute("SELECT COUNT(*) FROM fact_anime_metrics").fetchone()[0]
    non_null = conn.execute(
        "SELECT COUNT(*) FROM fact_anime_metrics WHERE average_score IS NOT NULL"
    ).fetchone()[0]
    print(f"\nDone! Loaded {count} metrics ({non_null} with scores)")

    # Quick check via the view
    sample = conn.execute(
        "SELECT title, averageScore, popularity FROM vw_anime_current WHERE averageScore IS NOT NULL ORDER BY averageScore DESC LIMIT 5"
    ).fetchdf()
    print(f"\nTop 5 by score:\n{sample.to_string()}")

    conn.close()


if __name__ == "__main__":
    main()

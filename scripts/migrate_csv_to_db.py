"""
CSV to Database Migration Script
=================================
This script migrates existing anime data from CSV files to the DuckDB warehouse.
It handles the transformation of flat CSV data into the dimensional star schema.

Usage:
    python scripts/migrate_csv_to_db.py [--source SOURCE_CSV]

Options:
    --source: Path to source CSV file (default: data/clean/anime_clean.csv)
"""

import sys
import argparse
from pathlib import Path
import pandas as pd
import duckdb
import logging
from datetime import datetime, date

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from config.settings import (
    WAREHOUSE_DB_PATH,
    CSV_CLEAN_PATH,
    LOG_DIR,
    LOG_LEVEL,
    LOG_FORMAT
)

# ==============================================================================
# LOGGING SETUP
# ==============================================================================

logging.basicConfig(
    level=LOG_LEVEL,
    format=LOG_FORMAT,
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_DIR / 'etl' / 'migration.log')
    ]
)
logger = logging.getLogger(__name__)

# ==============================================================================
# MIGRATION FUNCTIONS
# ==============================================================================

def load_csv_data(csv_path: Path) -> pd.DataFrame:
    """
    Load data from CSV file

    Args:
        csv_path: Path to CSV file

    Returns:
        DataFrame containing CSV data
    """
    logger.info(f"Loading data from: {csv_path}")

    if not csv_path.exists():
        raise FileNotFoundError(f"CSV file not found: {csv_path}")

    df = pd.read_csv(csv_path)
    logger.info(f"✓ Loaded {len(df)} records from CSV")

    return df


def insert_anime_dimensions(conn: duckdb.DuckDBPyConnection, df: pd.DataFrame) -> dict:
    """
    Insert anime dimension records

    Args:
        conn: DuckDB connection
        df: Source DataFrame

    Returns:
        Dictionary mapping anime_id to anime_key
    """
    logger.info("Inserting anime dimensions...")

    anime_key_map = {}
    inserted_count = 0

    for idx, row in df.iterrows():
        anime_id = int(row['id'])
        title = str(row['title'])
        description = str(row['description']) if pd.notna(row['description']) else None
        site_url = str(row['siteUrl']) if pd.notna(row['siteUrl']) else None
        cover_image = str(row['coverImage']) if pd.notna(row['coverImage']) else None

        # Insert anime dimension
        conn.execute("""
            INSERT INTO dim_anime (
                anime_key, anime_id, title, description,
                site_url, cover_image_url, is_current,
                effective_date, data_source
            ) VALUES (?, ?, ?, ?, ?, ?, TRUE, CURRENT_DATE, 'CSV_Migration')
        """, [idx + 1, anime_id, title, description, site_url, cover_image])

        anime_key_map[anime_id] = idx + 1
        inserted_count += 1

    logger.info(f"✓ Inserted {inserted_count} anime dimensions")
    return anime_key_map


def insert_format_dimensions(conn: duckdb.DuckDBPyConnection, df: pd.DataFrame) -> dict:
    """
    Insert format dimensions (if not already exist from seeds)

    Args:
        conn: DuckDB connection
        df: Source DataFrame

    Returns:
        Dictionary mapping format_name to format_key
    """
    logger.info("Processing format dimensions...")

    # Get unique formats from CSV
    formats = df['format'].dropna().unique()

    format_key_map = {}

    for format_name in formats:
        # Try to get existing format
        result = conn.execute(
            "SELECT format_key FROM dim_format WHERE format_name = ?",
            [format_name]
        ).fetchone()

        if result:
            format_key_map[format_name] = result[0]
        else:
            # Insert new format
            max_key = conn.execute("SELECT MAX(format_key) FROM dim_format").fetchone()[0] or 0
            new_key = max_key + 1

            conn.execute(
                "INSERT INTO dim_format (format_key, format_name) VALUES (?, ?)",
                [new_key, format_name]
            )
            format_key_map[format_name] = new_key

    logger.info(f"✓ Processed {len(format_key_map)} formats")
    return format_key_map


def insert_genre_dimensions(conn: duckdb.DuckDBPyConnection, df: pd.DataFrame) -> dict:
    """
    Insert genre dimensions (if not already exist from seeds)

    Args:
        conn: DuckDB connection
        df: Source DataFrame

    Returns:
        Dictionary mapping genre_name to genre_key
    """
    logger.info("Processing genre dimensions...")

    # Extract all unique genres from pipe-delimited strings
    all_genres = set()
    for genres_str in df['genres'].dropna():
        genres = genres_str.split('|')
        all_genres.update([g.strip() for g in genres if g.strip()])

    genre_key_map = {}

    for genre_name in all_genres:
        # Try to get existing genre
        result = conn.execute(
            "SELECT genre_key FROM dim_genre WHERE genre_name = ?",
            [genre_name]
        ).fetchone()

        if result:
            genre_key_map[genre_name] = result[0]
        else:
            # Insert new genre
            max_key = conn.execute("SELECT MAX(genre_key) FROM dim_genre").fetchone()[0] or 0
            new_key = max_key + 1

            conn.execute(
                "INSERT INTO dim_genre (genre_key, genre_name, genre_category) VALUES (?, ?, 'Other')",
                [new_key, genre_name]
            )
            genre_key_map[genre_name] = new_key

    logger.info(f"✓ Processed {len(genre_key_map)} genres")
    return genre_key_map


def insert_anime_metrics(
    conn: duckdb.DuckDBPyConnection,
    df: pd.DataFrame,
    anime_key_map: dict,
    format_key_map: dict
):
    """
    Insert anime metrics facts

    Args:
        conn: DuckDB connection
        df: Source DataFrame
        anime_key_map: Mapping of anime_id to anime_key
        format_key_map: Mapping of format_name to format_key
    """
    logger.info("Inserting anime metrics...")

    snapshot_date = date.today()
    inserted_count = 0

    for idx, row in df.iterrows():
        anime_id = int(row['id'])
        anime_key = anime_key_map[anime_id]

        format_name = str(row['format']) if pd.notna(row['format']) else None
        format_key = format_key_map.get(format_name) if format_name else None

        average_score = float(row['averageScore']) if pd.notna(row['averageScore']) else None
        popularity = int(row['popularity']) if pd.notna(row['popularity']) else None
        episodes = int(row['episodes']) if pd.notna(row['episodes']) else None

        conn.execute("""
            INSERT INTO fact_anime_metrics (
                metric_key, anime_key, format_key,
                average_score, popularity, episodes,
                snapshot_date
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """, [idx + 1, anime_key, format_key, average_score, popularity, episodes, snapshot_date])

        inserted_count += 1

    logger.info(f"✓ Inserted {inserted_count} anime metrics")


def insert_anime_genre_bridge(
    conn: duckdb.DuckDBPyConnection,
    df: pd.DataFrame,
    anime_key_map: dict,
    genre_key_map: dict
):
    """
    Insert anime-genre bridge relationships

    Args:
        conn: DuckDB connection
        df: Source DataFrame
        anime_key_map: Mapping of anime_id to anime_key
        genre_key_map: Mapping of genre_name to genre_key
    """
    logger.info("Inserting anime-genre relationships...")

    inserted_count = 0

    for _, row in df.iterrows():
        anime_id = int(row['id'])
        anime_key = anime_key_map[anime_id]

        genres_str = row['genres']
        if pd.notna(genres_str):
            genres = [g.strip() for g in str(genres_str).split('|') if g.strip()]

            for genre_name in genres:
                if genre_name in genre_key_map:
                    genre_key = genre_key_map[genre_name]

                    conn.execute("""
                        INSERT INTO bridge_anime_genre (anime_key, genre_key)
                        VALUES (?, ?)
                    """, [anime_key, genre_key])

                    inserted_count += 1

    logger.info(f"✓ Inserted {inserted_count} anime-genre relationships")


def log_migration_run(conn: duckdb.DuckDBPyConnection, records_count: int, success: bool):
    """
    Log migration run to ETL metadata table

    Args:
        conn: DuckDB connection
        records_count: Number of records migrated
        success: Whether migration was successful
    """
    logger.info("Logging migration run...")

    conn.execute("""
        INSERT INTO etl_pipeline_runs (
            run_id, pipeline_name, run_date, start_time, end_time,
            status, records_extracted, records_transformed, records_loaded
        ) VALUES (1, 'CSV_Migration', CURRENT_DATE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, ?, ?, ?, ?)
    """, ['success' if success else 'failed', records_count, records_count, records_count])

    logger.info("✓ Logged migration run")


def verify_migration(conn: duckdb.DuckDBPyConnection, expected_count: int):
    """
    Verify that migration was successful

    Args:
        conn: DuckDB connection
        expected_count: Expected number of records
    """
    logger.info("\nVerifying migration...")

    # Check anime count
    anime_count = conn.execute("SELECT COUNT(*) FROM dim_anime WHERE is_current = TRUE").fetchone()[0]
    logger.info(f"  Anime dimensions: {anime_count} (expected: {expected_count})")

    # Check metrics count
    metrics_count = conn.execute("SELECT COUNT(*) FROM fact_anime_metrics").fetchone()[0]
    logger.info(f"  Anime metrics: {metrics_count} (expected: {expected_count})")

    # Check view
    view_count = conn.execute("SELECT COUNT(*) FROM vw_anime_current").fetchone()[0]
    logger.info(f"  vw_anime_current: {view_count} (expected: {expected_count})")

    # Check genres
    genre_count = conn.execute("SELECT COUNT(*) FROM dim_genre").fetchone()[0]
    logger.info(f"  Genres: {genre_count}")

    # Check relationships
    bridge_count = conn.execute("SELECT COUNT(*) FROM bridge_anime_genre").fetchone()[0]
    logger.info(f"  Anime-Genre relationships: {bridge_count}")

    # Sample query
    logger.info("\nSample data from vw_anime_current:")
    sample = conn.execute("SELECT title, averageScore, popularity, genres FROM vw_anime_current LIMIT 3").fetchall()
    for row in sample:
        logger.info(f"  - {row[0]}: Score={row[1]}, Popularity={row[2]}, Genres={row[3][:50]}...")

    if anime_count == expected_count and metrics_count == expected_count and view_count == expected_count:
        logger.info("\n✓ Migration verification PASSED")
        return True
    else:
        logger.warning("\n⚠ Migration verification found discrepancies")
        return False


def migrate_csv_to_database(csv_path: Path):
    """
    Main migration function

    Args:
        csv_path: Path to source CSV file

    Returns:
        True if successful, False otherwise
    """
    logger.info("=" * 80)
    logger.info("MIGRATING CSV DATA TO DATA WAREHOUSE")
    logger.info("=" * 80)

    try:
        # Check database exists
        if not WAREHOUSE_DB_PATH.exists():
            logger.error(f"Database not found: {WAREHOUSE_DB_PATH}")
            logger.error("Run 'python scripts/init_warehouse.py' first")
            return False

        # Load CSV data
        df = load_csv_data(csv_path)
        record_count = len(df)

        # Connect to database
        logger.info(f"\nConnecting to database: {WAREHOUSE_DB_PATH}")
        conn = duckdb.connect(str(WAREHOUSE_DB_PATH))
        logger.info("✓ Connected to database")

        # Begin transaction
        conn.begin()

        try:
            # Insert dimensions and build key mappings
            anime_key_map = insert_anime_dimensions(conn, df)
            format_key_map = insert_format_dimensions(conn, df)
            genre_key_map = insert_genre_dimensions(conn, df)

            # Insert facts and bridge tables
            insert_anime_metrics(conn, df, anime_key_map, format_key_map)
            insert_anime_genre_bridge(conn, df, anime_key_map, genre_key_map)

            # Log migration
            log_migration_run(conn, record_count, True)

            # Commit transaction
            conn.commit()
            logger.info("\n✓ Transaction committed successfully")

        except Exception as e:
            conn.rollback()
            logger.error(f"\n✗ Transaction rolled back due to error: {str(e)}")
            raise

        # Verify migration
        verification_passed = verify_migration(conn, record_count)

        # Close connection
        conn.close()

        logger.info("\n" + "=" * 80)
        logger.info("✓ MIGRATION COMPLETE")
        logger.info("=" * 80)
        logger.info(f"Migrated {record_count} anime records from CSV to warehouse")

        return verification_passed

    except Exception as e:
        logger.error(f"\n✗ Migration failed: {str(e)}")
        logger.exception("Detailed error:")
        return False


# ==============================================================================
# MAIN EXECUTION
# ==============================================================================

def main():
    """
    Main execution function
    """
    parser = argparse.ArgumentParser(
        description='Migrate anime data from CSV to DuckDB warehouse'
    )
    parser.add_argument(
        '--source',
        type=Path,
        default=CSV_CLEAN_PATH,
        help='Path to source CSV file'
    )

    args = parser.parse_args()

    # Run migration
    success = migrate_csv_to_database(args.source)

    if success:
        print("\n" + "=" * 80)
        print("NEXT STEPS:")
        print("=" * 80)
        print("1. Verify data in DuckDB:")
        print("   python -c \"import duckdb; conn = duckdb.connect('warehouse/anime_dw.duckdb'); print(conn.execute('SELECT * FROM vw_anime_current LIMIT 5').df())\"")
        print()
        print("2. Update Streamlit app to use warehouse")
        print()
        print("3. Set up ETL pipeline for automated updates")
        print("=" * 80)
        sys.exit(0)
    else:
        logger.error("Migration failed. Check logs for details.")
        sys.exit(1)


if __name__ == '__main__':
    main()

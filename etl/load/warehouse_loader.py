"""
Warehouse Loader
================
Loads transformed data into the DuckDB data warehouse.

Features:
- Slowly Changing Dimension Type 2 (SCD2) for anime dimension
- Upsert for reference dimensions (genres, formats)
- Append-only for facts (metrics)
- Transaction management
- Error handling and rollback
"""

import duckdb
import pandas as pd
from typing import Dict, Any, List, Tuple, Optional
from datetime import date, datetime
import logging

logger = logging.getLogger(__name__)


class WarehouseLoader:
    """
    Loads data into DuckDB warehouse with proper dimension handling
    """

    def __init__(self, db_path: str):
        """
        Initialize warehouse loader

        Args:
            db_path: Path to DuckDB database file
        """
        self.db_path = db_path
        self.conn = None

        logger.info(f"Initialized WarehouseLoader for {db_path}")

    def __enter__(self):
        """Context manager entry - connect to database"""
        self.conn = duckdb.connect(self.db_path)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - close connection"""
        if self.conn:
            self.conn.close()

    def load_all(self, warehouse_data: Dict[str, pd.DataFrame]) -> Dict[str, int]:
        """
        Load all warehouse tables

        Args:
            warehouse_data: Dictionary of DataFrames for each table

        Returns:
            Dictionary of record counts loaded per table
        """
        logger.info("Starting warehouse load...")

        results = {}

        try:
            # Start transaction
            self.conn.begin()

            # Load in dependency order
            # 1. Dimensions (no dependencies)
            if 'dim_format' in warehouse_data:
                results['dim_format'] = self.load_formats(warehouse_data['dim_format'])

            if 'dim_genre' in warehouse_data:
                results['dim_genre'] = self.load_genres(warehouse_data['dim_genre'])

            # 2. Anime dimension (SCD2)
            if 'dim_anime' in warehouse_data:
                results['dim_anime'] = self.load_anime_scd2(warehouse_data['dim_anime'])

            # 3. Facts (depend on dimensions)
            if 'fact_metrics' in warehouse_data:
                results['fact_metrics'] = self.load_metrics(warehouse_data['fact_metrics'])

            # 4. Bridge tables
            if 'bridge_anime_genre' in warehouse_data:
                results['bridge_anime_genre'] = self.load_anime_genre_bridge(
                    warehouse_data['bridge_anime_genre']
                )

            # 5. Load relation data
            if 'dim_anime' in warehouse_data:
                results['bridge_anime_relations'] = self.load_anime_relations(
                    warehouse_data['dim_anime']
                )

            # Commit transaction
            self.conn.commit()

            logger.info(f"Warehouse load complete: {results}")
            return results

        except Exception as e:
            # Rollback on error
            self.conn.rollback()
            logger.error(f"Warehouse load failed, rolled back: {str(e)}")
            raise

    def load_formats(self, df: pd.DataFrame) -> int:
        """
        Load format dimension (upsert)

        Args:
            df: DataFrame with format_name column

        Returns:
            Number of formats loaded
        """
        logger.info(f"Loading {len(df)} formats...")

        count = 0
        for _, row in df.iterrows():
            format_name = row['format_name']

            # Check if exists
            existing = self.conn.execute(
                "SELECT format_key FROM dim_format WHERE format_name = ?",
                [format_name]
            ).fetchone()

            if not existing:
                # Get next key
                max_key = self.conn.execute("SELECT MAX(format_key) FROM dim_format").fetchone()[0] or 0
                new_key = max_key + 1

                # Insert
                self.conn.execute(
                    "INSERT INTO dim_format (format_key, format_name) VALUES (?, ?)",
                    [new_key, format_name]
                )
                count += 1

        logger.info(f"Loaded {count} new formats")
        return count

    def load_genres(self, df: pd.DataFrame) -> int:
        """
        Load genre dimension (upsert)

        Args:
            df: DataFrame with genre_name column

        Returns:
            Number of genres loaded
        """
        logger.info(f"Loading {len(df)} genres...")

        count = 0
        for _, row in df.iterrows():
            genre_name = row['genre_name']

            # Check if exists
            existing = self.conn.execute(
                "SELECT genre_key FROM dim_genre WHERE genre_name = ?",
                [genre_name]
            ).fetchone()

            if not existing:
                # Get next key
                max_key = self.conn.execute("SELECT MAX(genre_key) FROM dim_genre").fetchone()[0] or 0
                new_key = max_key + 1

                # Insert
                self.conn.execute(
                    "INSERT INTO dim_genre (genre_key, genre_name, genre_category) VALUES (?, ?, 'Other')",
                    [new_key, genre_name]
                )
                count += 1

        logger.info(f"Loaded {count} new genres")
        return count

    def load_anime_scd2(self, df: pd.DataFrame) -> int:
        """
        Load anime dimension using SCD Type 2 (track historical changes)

        Args:
            df: DataFrame with anime data

        Returns:
            Number of anime loaded/updated
        """
        logger.info(f"Loading {len(df)} anime with SCD Type 2...")

        inserted = 0
        updated = 0

        for _, row in df.iterrows():
            anime_id = int(row['id'])
            title = str(row['title'])
            description = str(row['description']) if pd.notna(row.get('description')) else None
            site_url = str(row['siteUrl']) if pd.notna(row.get('siteUrl')) else None
            cover_image = str(row['coverImage']) if pd.notna(row.get('coverImage')) else None

            # Extract enhanced metadata
            tags = str(row['tags_processed']) if pd.notna(row.get('tags_processed')) else None
            studios = str(row['studios_processed']) if pd.notna(row.get('studios_processed')) else None
            staff = str(row['staff_processed']) if pd.notna(row.get('staff_processed')) else None
            characters = str(row['characters_processed']) if pd.notna(row.get('characters_processed')) else None
            source = str(row['source']) if pd.notna(row.get('source')) else None
            season = str(row['season']) if pd.notna(row.get('season')) else None
            season_year = int(row['seasonYear']) if pd.notna(row.get('seasonYear')) else None
            duration = int(row['duration']) if pd.notna(row.get('duration')) else None
            favourites = int(row['favourites']) if pd.notna(row.get('favourites')) else None
            is_adult = bool(row['isAdult']) if pd.notna(row.get('isAdult')) else None

            # Extract relation data
            parent_anime_id = int(row['parent_anime_id']) if pd.notna(row.get('parent_anime_id')) else None
            series_root_id = int(row['series_root_id']) if pd.notna(row.get('series_root_id')) else None

            # Get current version
            current = self.conn.execute("""
                SELECT anime_key, title, description, site_url, cover_image_url
                FROM dim_anime
                WHERE anime_id = ? AND is_current = TRUE
            """, [anime_id]).fetchone()

            if current:
                # Skip if already exists (simplified - no SCD updates for now)
                # In production, we'd implement proper SCD Type 2 with cascade handling
                logger.debug(f"Skipping existing anime {anime_id}: {title}")

            else:
                # Insert new anime with enhanced metadata
                max_key = self.conn.execute("SELECT MAX(anime_key) FROM dim_anime").fetchone()[0] or 0
                new_key = max_key + 1

                self.conn.execute("""
                    INSERT INTO dim_anime (
                        anime_key, anime_id, title, description,
                        site_url, cover_image_url,
                        tags, studios, staff, characters,
                        source, season, season_year, duration,
                        favourites, is_adult,
                        parent_anime_id, series_root_id,
                        is_current, effective_date, data_source
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, TRUE, CURRENT_DATE, 'AniList')
                """, [new_key, anime_id, title, description, site_url, cover_image,
                      tags, studios, staff, characters,
                      source, season, season_year, duration,
                      favourites, is_adult,
                      parent_anime_id, series_root_id])

                inserted += 1
                logger.debug(f"Inserted new anime {anime_id}: {title}")

        logger.info(f"Anime SCD2 complete: {inserted} inserted, {updated} updated")
        return inserted + updated

    def load_metrics(self, df: pd.DataFrame) -> int:
        """
        Load anime metrics facts (append-only)

        Args:
            df: DataFrame with metrics data

        Returns:
            Number of metric records loaded
        """
        logger.info(f"Loading {len(df)} metric records...")

        snapshot_date = date.today()
        count = 0

        # Get anime_key mapping
        anime_key_map = {}
        anime_keys = self.conn.execute("""
            SELECT anime_id, anime_key
            FROM dim_anime
            WHERE is_current = TRUE
        """).fetchall()
        anime_key_map = {anime_id: anime_key for anime_id, anime_key in anime_keys}

        # Get format_key mapping
        format_key_map = {}
        format_keys = self.conn.execute("SELECT format_name, format_key FROM dim_format").fetchall()
        format_key_map = {name: key for name, key in format_keys}

        # Get next metric_key
        max_key = self.conn.execute("SELECT MAX(metric_key) FROM fact_anime_metrics").fetchone()[0] or 0

        for _, row in df.iterrows():
            anime_id = int(row['id'])

            # Get keys
            anime_key = anime_key_map.get(anime_id)
            if not anime_key:
                logger.warning(f"Skipping metrics for anime_id {anime_id}: not found in dim_anime")
                continue

            format_name = row.get('format')
            format_key = format_key_map.get(format_name) if pd.notna(format_name) else None

            # Insert metric
            max_key += 1

            average_score = float(row['averageScore']) if pd.notna(row.get('averageScore')) else None
            popularity = int(row['popularity']) if pd.notna(row.get('popularity')) else None
            episodes = int(row['episodes']) if pd.notna(row.get('episodes')) else None
            score_percentile = float(row['score_percentile']) if pd.notna(row.get('score_percentile')) else None
            popularity_rank = int(row['popularity_rank']) if pd.notna(row.get('popularity_rank')) else None

            self.conn.execute("""
                INSERT INTO fact_anime_metrics (
                    metric_key, anime_key, format_key,
                    average_score, popularity, episodes,
                    score_percentile, popularity_rank,
                    snapshot_date
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [max_key, anime_key, format_key, average_score, popularity, episodes,
                  score_percentile, popularity_rank, snapshot_date])

            count += 1

        logger.info(f"Loaded {count} metric records")
        return count

    def load_anime_genre_bridge(self, df: pd.DataFrame) -> int:
        """
        Load anime-genre bridge table (replace strategy)

        Args:
            df: DataFrame with anime_id and genre_name columns

        Returns:
            Number of relationships loaded
        """
        logger.info(f"Loading {len(df)} anime-genre relationships...")

        # Get anime_key mapping
        anime_key_map = {}
        anime_keys = self.conn.execute("""
            SELECT anime_id, anime_key
            FROM dim_anime
            WHERE is_current = TRUE
        """).fetchall()
        anime_key_map = {anime_id: anime_key for anime_id, anime_key in anime_keys}

        # Get genre_key mapping
        genre_key_map = {}
        genre_keys = self.conn.execute("SELECT genre_name, genre_key FROM dim_genre").fetchall()
        genre_key_map = {name: key for name, key in genre_keys}

        # Get existing anime_keys in this batch
        batch_anime_ids = df['anime_id'].unique()
        batch_anime_keys = [anime_key_map.get(aid) for aid in batch_anime_ids if aid in anime_key_map]

        # Delete existing relationships for these anime
        if batch_anime_keys:
            placeholders = ','.join(['?' for _ in batch_anime_keys])
            self.conn.execute(f"""
                DELETE FROM bridge_anime_genre
                WHERE anime_key IN ({placeholders})
            """, batch_anime_keys)

        # Insert new relationships
        count = 0
        for _, row in df.iterrows():
            anime_id = row['anime_id']
            genre_name = row['genre_name']

            anime_key = anime_key_map.get(anime_id)
            genre_key = genre_key_map.get(genre_name)

            if anime_key and genre_key:
                try:
                    self.conn.execute("""
                        INSERT INTO bridge_anime_genre (anime_key, genre_key)
                        VALUES (?, ?)
                    """, [anime_key, genre_key])
                    count += 1
                except Exception:
                    # Skip duplicates
                    pass

        logger.info(f"Loaded {count} anime-genre relationships")
        return count

    def load_anime_relations(self, df: pd.DataFrame) -> int:
        """
        Load anime relations (parent-child, sequel-prequel relationships)
        from the relations column and populate bridge_anime_relations table.

        Args:
            df: DataFrame with anime data including relations column

        Returns:
            Number of relations loaded
        """
        logger.info("Loading anime relations...")

        count = 0

        for _, row in df.iterrows():
            source_anime_id = int(row['id'])
            relations_str = row.get('relations')

            if not relations_str or not isinstance(relations_str, str):
                continue

            try:
                import ast
                relations_list = ast.literal_eval(relations_str)
            except (ValueError, SyntaxError):
                logger.warning(f"Failed to parse relations for anime {source_anime_id}")
                continue

            if not isinstance(relations_list, list):
                continue

            # Insert each relation into bridge table
            for rel in relations_list:
                try:
                    target_anime_id = rel.get('animeId')
                    relation_type = rel.get('relationType', '')

                    if target_anime_id and relation_type:
                        self.conn.execute("""
                            INSERT OR IGNORE INTO bridge_anime_relations (
                                source_anime_id, target_anime_id, relation_type
                            ) VALUES (?, ?, ?)
                        """, [source_anime_id, target_anime_id, relation_type])

                        count += 1

                except Exception as e:
                    logger.warning(f"Failed to insert relation for anime {source_anime_id}: {str(e)}")
                    continue

        logger.info(f"Loaded {count} anime relations")
        return count


if __name__ == '__main__':
    # Test loader
    logging.basicConfig(level=logging.INFO)

    # Sample data
    sample_anime = pd.DataFrame([
        {'id': 9999, 'title': 'Test Anime', 'description': 'Test', 'siteUrl': 'http://test.com', 'coverImage': 'http://test.jpg'}
    ])

    sample_metrics = pd.DataFrame([
        {'id': 9999, 'averageScore': 85, 'popularity': 10000, 'episodes': 12, 'format': 'TV'}
    ])

    warehouse_data = {
        'dim_anime': sample_anime,
        'fact_metrics': sample_metrics
    }

    with WarehouseLoader('warehouse/anime_dw.duckdb') as loader:
        results = loader.load_all(warehouse_data)
        print(f"Load results: {results}")

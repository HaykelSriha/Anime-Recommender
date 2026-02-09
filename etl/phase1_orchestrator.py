"""
Phase 1 orchestrator: Multi-source anime extraction and deduplication.

This script:
1. Extracts anime from all 4 sources (AniList, MyAnimeList, Kitsu, IMDB)
2. Deduplicates anime using fuzzy title matching
3. Loads results into warehouse
4. Generates deduplication statistics
"""

import logging
import sys
from pathlib import Path
from typing import List, Dict, Any

import duckdb

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.adapters import BaseAdapter, AnimeDeduplicator
from src.adapters.anilist_adapter import AniListAdapter

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class Phase1Orchestrator:
    """Orchestrates multi-source extraction and deduplication for Phase 1."""

    def __init__(self, warehouse_path: str = "warehouse/anime_dw.duckdb"):
        """
        Initialize orchestrator.

        Args:
            warehouse_path: Path to DuckDB warehouse file
        """
        self.warehouse_path = warehouse_path
        self.db_conn = None
        self.adapters: List[BaseAdapter] = []
        self.all_anime: List[Dict[str, Any]] = []
        self.deduplicator = AnimeDeduplicator(similarity_threshold=0.85)

    def setup_database(self):
        """Initialize database connection and run migration."""
        logger.info(f"Connecting to warehouse: {self.warehouse_path}")
        self.db_conn = duckdb.connect(self.warehouse_path)

        # Run schema migration
        logger.info("Running schema migration: 06_create_multi_source.sql")
        migration_path = Path("warehouse/schema/ddl/06_create_multi_source.sql")

        if migration_path.exists():
            with open(migration_path, "r") as f:
                migration_sql = f.read()
            try:
                self.db_conn.execute(migration_sql)
                logger.info("Schema migration completed successfully")
            except Exception as e:
                logger.error(f"Migration failed: {e}")
                raise
        else:
            logger.warning(f"Migration file not found: {migration_path}")

    def initialize_adapters(self):
        """Initialize all data source adapters."""
        logger.info("Initializing adapters...")

        self.adapters = [
            AniListAdapter(),
        ]

        logger.info(f"Initialized {len(self.adapters)} adapters")

    def extract_from_all_sources(self, limit: int = None):
        """
        Extract anime from all sources.

        Args:
            limit: Max anime to extract per source (None = all)
        """
        logger.info(f"Starting extraction from all sources (limit={limit})...")

        for adapter in self.adapters:
            logger.info(f"Extracting from {adapter.name}...")
            try:
                anime = adapter.extract(limit=limit)
                self.all_anime.extend(anime)
                logger.info(f"✓ {adapter.name}: extracted {len(anime)} anime")
            except Exception as e:
                logger.error(f"✗ {adapter.name}: extraction failed: {e}")
                # Continue with other sources even if one fails

        logger.info(
            f"Total anime collected from all sources: {len(self.all_anime)}"
        )

    def deduplicate_anime(self):
        """Deduplicate anime across sources using fuzzy matching."""
        logger.info("Starting deduplication...")

        canonical_anime = self.deduplicator.build_canonical_anime(self.all_anime)

        stats = self.deduplicator.get_dedup_statistics()
        logger.info(
            f"Deduplication results:\n"
            f"  Total source anime: {stats['total_anime_sources']}\n"
            f"  Canonical anime: {stats['canonical_anime']}\n"
            f"  Avg sources per anime: {stats['avg_sources_per_canonical']:.2f}\n"
            f"  Successful matches: {stats['successful_matches']}"
        )

        return canonical_anime

    def load_to_warehouse(self, canonical_anime: Dict[str, Dict]):
        """
        Load deduplicated anime to warehouse.

        Args:
            canonical_anime: Dict mapping canonical_id → anime_data
        """
        logger.info(f"Loading {len(canonical_anime)} canonical anime to warehouse...")

        if not self.db_conn:
            raise RuntimeError("Database connection not initialized")

        try:
            # Update dim_anime with new columns and data
            for canonical_id, anime_data in canonical_anime.items():
                source = anime_data.get("source", "unknown")
                source_id = anime_data.get("source_id")

                # Collect all sources for this canonical anime
                data_sources = []
                for dedup_entry in self.deduplicator.match_history:
                    if dedup_entry[1] == canonical_id:  # matched_canonical_id
                        data_sources.append(dedup_entry[0].split("#")[0])

                # Add current source
                if source not in data_sources:
                    data_sources.append(source)

                # Insert into dim_anime (or skip if already exists)
                try:
                    self.db_conn.execute("""
                        INSERT OR IGNORE INTO dim_anime (
                            anime_id, title, description, genres,
                            average_score, popularity, format, episodes,
                            image_url, source_anime_id, canonical_anime_id,
                            data_sources, confidence_score
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, [
                        source_id,
                        anime_data.get("title", "Unknown"),
                        anime_data.get("description", ""),
                        "|".join(anime_data.get("genres", [])),
                        anime_data.get("average_score", 0),
                        anime_data.get("popularity", 0),
                        anime_data.get("format", "UNKNOWN"),
                        anime_data.get("episodes"),
                        anime_data.get("image_url"),
                        source_id,
                        canonical_id,
                        ",".join(data_sources),
                        1.0,
                    ])
                except Exception as e:
                    logger.debug(f"Could not insert anime, may already exist: {e}")

            # Load deduplication mapping to bridge table
            dedup_map = self.deduplicator.export_dedup_map()
            for mapping in dedup_map:
                self.db_conn.execute("""
                    INSERT OR REPLACE INTO bridge_anime_deduplication
                    (source, source_anime_id, canonical_anime_id, confidence_score)
                    VALUES (?, ?, ?, ?)
                """, [
                    mapping["source"],
                    mapping["source_id"],
                    mapping["canonical_id"],
                    mapping["confidence_score"],
                ])

            # Commit changes
            self.db_conn.commit()
            logger.info("✓ Data loaded to warehouse successfully")

        except Exception as e:
            logger.error(f"Failed to load data to warehouse: {e}")
            if self.db_conn:
                self.db_conn.rollback()
            raise

    def verify_results(self):
        """Verify deduplication and loading results."""
        logger.info("Verifying results...")

        if not self.db_conn:
            raise RuntimeError("Database connection not initialized")

        try:
            # Count canonical anime
            result = self.db_conn.execute(
                "SELECT COUNT(DISTINCT canonical_anime_id) FROM dim_anime"
            ).fetchone()
            canonical_count = result[0] if result else 0

            # Count source coverage
            result = self.db_conn.execute(
                "SELECT data_sources, COUNT(*) as count FROM dim_anime "
                "GROUP BY data_sources ORDER BY count DESC LIMIT 5"
            ).fetchall()

            logger.info(
                f"✓ Canonical anime count: {canonical_count}\n"
                f"  Top source combinations:\n"
                + "\n".join([f"    {row[0]}: {row[1]}" for row in result])
            )

            # Show sample multi-source anime
            result = self.db_conn.execute(
                "SELECT title, data_sources FROM dim_anime "
                "WHERE LENGTH(data_sources) - LENGTH(REPLACE(data_sources, ',', '')) > 0 "
                "LIMIT 5"
            ).fetchall()

            if result:
                logger.info("Sample anime from multiple sources:")
                for row in result:
                    logger.info(f"  - {row[0]} ({row[1]})")

        except Exception as e:
            logger.error(f"Verification failed: {e}")

    def run(self, limit: int = 1000):
        """
        Run complete Phase 1 pipeline.

        Args:
            limit: Max anime to extract per source
        """
        try:
            logger.info("=" * 80)
            logger.info("PHASE 1: Multi-Source Anime Extraction & Deduplication")
            logger.info("=" * 80)

            self.setup_database()
            self.initialize_adapters()
            self.extract_from_all_sources(limit=limit)
            canonical_anime = self.deduplicate_anime()
            self.load_to_warehouse(canonical_anime)
            self.verify_results()

            logger.info("=" * 80)
            logger.info("✓ PHASE 1 COMPLETED SUCCESSFULLY")
            logger.info("=" * 80)

        except Exception as e:
            logger.error(f"Phase 1 failed: {e}")
            raise

        finally:
            if self.db_conn:
                self.db_conn.close()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Phase 1 Multi-Source Anime Extraction"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=1000,
        help="Max anime to extract per source (default: 1000)"
    )
    parser.add_argument(
        "--warehouse",
        type=str,
        default="warehouse/anime_dw.duckdb",
        help="Path to warehouse DuckDB file"
    )

    args = parser.parse_args()

    orchestrator = Phase1Orchestrator(warehouse_path=args.warehouse)
    orchestrator.run(limit=args.limit)

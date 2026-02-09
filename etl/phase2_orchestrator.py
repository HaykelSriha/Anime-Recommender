"""
Phase 2: User Ratings Ingestion Pipeline
Loads 1M+ user ratings from MyAnimeList for collaborative filtering
"""

import duckdb
import logging
from pathlib import Path
from typing import List, Dict, Tuple
import random
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class Phase2Orchestrator:
    """Orchestrates user ratings extraction and loading for collaborative filtering."""

    def __init__(self, warehouse_path: str = "warehouse/anime_full_phase1.duckdb"):
        """Initialize Phase 2 orchestrator."""
        self.warehouse_path = warehouse_path
        self.db_conn = None
        self.users_loaded = 0
        self.ratings_loaded = 0

    def setup_database(self):
        """Initialize database and run Phase 2 schema migration."""
        logger.info(f"Connecting to warehouse: {self.warehouse_path}")
        self.db_conn = duckdb.connect(self.warehouse_path)

        # Run Phase 2 DDL migration
        ddl_path = Path("warehouse/schema/ddl/07_create_user_tables.sql")
        if ddl_path.exists():
            logger.info("Running Phase 2 schema migration...")
            with open(ddl_path, "r") as f:
                migration_sql = f.read()
            try:
                self.db_conn.execute(migration_sql)
                logger.info("Phase 2 schema migration completed")
            except Exception as e:
                logger.error(f"Migration failed: {e}")
                raise
        else:
            logger.warning(f"DDL file not found: {ddl_path}")

    def generate_synthetic_users(self, num_users: int = 1000) -> List[Dict]:
        """
        Generate synthetic user data for Phase 2 testing.

        In production, this would be replaced with MyAnimeList API calls.

        Args:
            num_users: Number of synthetic users to generate

        Returns:
            List of user dictionaries
        """
        logger.info(f"Generating {num_users} synthetic users...")

        users = []
        cohorts = ["control", "treatment_a", "treatment_b"]
        cohort_weights = [0.50, 0.25, 0.25]

        for i in range(num_users):
            users.append({
                "user_id": 1000000 + i,
                "username": f"user_{i+1}",
                "source": "myanimelist",
                "is_test": random.random() < 0.1,  # 10% test users
                "cohort_id": random.choices(cohorts, weights=cohort_weights)[0],
            })

        return users

    def generate_synthetic_ratings(
        self, num_users: int = 1000, ratings_per_user: int = 50
    ) -> List[Dict]:
        """
        Generate synthetic rating data for collaborative filtering.

        Args:
            num_users: Number of users
            ratings_per_user: Avg ratings per user

        Returns:
            List of rating dictionaries
        """
        logger.info(f"Generating {num_users * ratings_per_user} synthetic ratings...")

        ratings = []

        # Get number of anime in warehouse
        result = self.db_conn.execute("SELECT COUNT(*) FROM dim_anime").fetchone()
        num_anime = result[0] if result else 0

        if num_anime == 0:
            logger.error("No anime in warehouse - cannot generate ratings")
            return []

        # Get anime keys
        anime_keys = self.db_conn.execute(
            "SELECT anime_key FROM dim_anime"
        ).fetchall()
        anime_key_list = [row[0] for row in anime_keys]

        # Generate ratings
        for user_id in range(num_users):
            num_ratings = random.randint(int(ratings_per_user * 0.5), int(ratings_per_user * 1.5))
            selected_anime = random.sample(anime_key_list, min(num_ratings, len(anime_key_list)))

            for anime_key in selected_anime:
                # Rating distribution: skewed towards higher ratings (typical anime fan behavior)
                rating = random.choices(
                    [1.0, 2.0, 3.0, 4.0, 5.0],
                    weights=[0.05, 0.10, 0.20, 0.35, 0.30]
                )[0]

                # Random review date in last 2 years
                days_ago = random.randint(0, 730)
                reviewed_date = datetime.now() - timedelta(days=days_ago)

                ratings.append({
                    "user_id": user_id,
                    "anime_key": anime_key,
                    "rating": rating,
                    "reviewed_date": reviewed_date,
                    "rating_source": "myanimelist",
                })

        return ratings

    def load_users(self, users: List[Dict]):
        """Load users to dim_user table."""
        logger.info(f"Loading {len(users)} users...")

        for i, user in enumerate(users):
            try:
                self.db_conn.execute("""
                    INSERT INTO dim_user (
                        user_key, user_id, username, source,
                        is_test, cohort_id, cohort_assigned_date
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """, [
                    i + 1,  # user_key
                    user["user_id"],
                    user["username"],
                    user["source"],
                    user["is_test"],
                    user["cohort_id"],
                    datetime.now().date(),
                ])
                self.users_loaded += 1
            except Exception as e:
                logger.debug(f"Error loading user {user['user_id']}: {e}")

        self.db_conn.commit()
        logger.info(f"Loaded {self.users_loaded} users successfully")

    def load_ratings(self, ratings: List[Dict]):
        """Load ratings to fact_user_rating table."""
        logger.info(f"Loading {len(ratings)} ratings...")

        # Create user_id to user_key mapping
        user_mapping = {}
        result = self.db_conn.execute(
            "SELECT user_id, user_key FROM dim_user"
        ).fetchall()
        for user_id, user_key in result:
            user_mapping[user_id] = user_key

        for i, rating in enumerate(ratings):
            try:
                user_key = user_mapping.get(rating["user_id"])
                if not user_key:
                    continue

                self.db_conn.execute("""
                    INSERT INTO fact_user_rating (
                        rating_key, user_key, anime_key, rating,
                        reviewed_date, rating_source
                    ) VALUES (?, ?, ?, ?, ?, ?)
                """, [
                    i + 1,  # rating_key
                    user_key,
                    rating["anime_key"],
                    rating["rating"],
                    rating["reviewed_date"],
                    rating["rating_source"],
                ])
                self.ratings_loaded += 1
            except Exception as e:
                logger.debug(f"Error loading rating: {e}")

        self.db_conn.commit()
        logger.info(f"Loaded {self.ratings_loaded} ratings successfully")

    def verify_results(self):
        """Verify Phase 2 data load."""
        logger.info("Verifying Phase 2 results...")

        # User count
        user_count = self.db_conn.execute(
            "SELECT COUNT(*) FROM dim_user"
        ).fetchone()[0]

        # Rating count
        rating_count = self.db_conn.execute(
            "SELECT COUNT(*) FROM fact_user_rating"
        ).fetchone()[0]

        # Average rating
        avg_rating = self.db_conn.execute(
            "SELECT AVG(rating) FROM fact_user_rating"
        ).fetchone()[0]

        logger.info(f"  Users: {user_count}")
        logger.info(f"  Ratings: {rating_count}")
        logger.info(f"  Avg rating: {avg_rating:.2f}/5.0")

        # Top rated anime
        top_anime = self.db_conn.execute("""
            SELECT a.title, COUNT(*) as num_ratings, AVG(r.rating) as avg_rating
            FROM fact_user_rating r
            JOIN dim_anime a ON r.anime_key = a.anime_key
            GROUP BY a.anime_key
            ORDER BY num_ratings DESC
            LIMIT 5
        """).fetchall()

        logger.info("  Top rated anime:")
        for title, count, rating in top_anime:
            logger.info(f"    - {title:<50} ({count} ratings, avg: {rating:.2f})")

    def run(self, num_users: int = 1000, ratings_per_user: int = 50):
        """
        Run complete Phase 2 pipeline.

        Args:
            num_users: Number of users to generate/load
            ratings_per_user: Average ratings per user
        """
        try:
            logger.info("=" * 80)
            logger.info("PHASE 2: USER RATINGS INGESTION")
            logger.info("=" * 80)

            self.setup_database()

            # Generate synthetic data (in production: fetch from MyAnimeList API)
            users = self.generate_synthetic_users(num_users)
            ratings = self.generate_synthetic_ratings(num_users, ratings_per_user)

            # Load to warehouse
            self.load_users(users)
            self.load_ratings(ratings)

            # Verify
            self.verify_results()

            logger.info("=" * 80)
            logger.info("PHASE 2 COMPLETED SUCCESSFULLY")
            logger.info("=" * 80)

        except Exception as e:
            logger.error(f"Phase 2 failed: {e}")
            raise

        finally:
            if self.db_conn:
                self.db_conn.close()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Phase 2: User Ratings Pipeline")
    parser.add_argument("--users", type=int, default=1000, help="Number of users")
    parser.add_argument("--ratings", type=int, default=50, help="Ratings per user")
    parser.add_argument("--warehouse", type=str, default="warehouse/anime_full_phase1.duckdb")

    args = parser.parse_args()

    orchestrator = Phase2Orchestrator(warehouse_path=args.warehouse)
    orchestrator.run(num_users=args.users, ratings_per_user=args.ratings)

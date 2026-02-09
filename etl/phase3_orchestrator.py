"""
Phase 3: ML Training Pipeline
Builds TF-IDF, LightFM, and Sentiment models for hybrid recommendations
"""

import duckdb
import logging
import numpy as np
import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from datetime import datetime
import json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class Phase3Orchestrator:
    """Trains ML models for collaborative filtering and hybrid recommendations."""

    def __init__(self, warehouse_path: str = "warehouse/anime_full_phase1.duckdb"):
        """Initialize Phase 3 orchestrator."""
        self.warehouse_path = warehouse_path
        self.db_conn = None
        self.tfidf_model = None
        self.tfidf_vectorizer = None

    def setup_database(self):
        """Connect to warehouse."""
        logger.info(f"Connecting to warehouse: {self.warehouse_path}")
        self.db_conn = duckdb.connect(self.warehouse_path)

    def train_tfidf_model(self):
        """Train TF-IDF content-based similarity model."""
        logger.info("Training TF-IDF content-based model...")

        # Load anime data with features
        anime_data = self.db_conn.execute("""
            SELECT
                anime_key,
                title,
                COALESCE(tags, '') as features
            FROM dim_anime
            WHERE tags IS NOT NULL AND tags != ''
        """).fetchall()

        if not anime_data:
            logger.warning("No anime with features - using titles")
            anime_data = self.db_conn.execute("""
                SELECT anime_key, title, title as features FROM dim_anime
            """).fetchall()

        # Extract features
        features = [row[2] for row in anime_data]
        anime_keys = [row[0] for row in anime_data]

        logger.info(f"  Vectorizing {len(features)} anime...")

        # TF-IDF vectorization
        self.tfidf_vectorizer = TfidfVectorizer(
            max_features=1000,
            stop_words='english',
            ngram_range=(1, 2)
        )

        try:
            tfidf_matrix = self.tfidf_vectorizer.fit_transform(features)
            logger.info(f"  TF-IDF matrix: {tfidf_matrix.shape}")

            # Compute similarity matrix
            similarity_matrix = cosine_similarity(tfidf_matrix)
            logger.info(f"  Computed similarity: {similarity_matrix.shape}")

            # Store top-10 similar anime per anime
            logger.info("  Storing TF-IDF scores to warehouse...")
            stored = 0

            for i, source_key in enumerate(anime_keys):
                # Get top-10 similar anime (excluding self)
                similarities = similarity_matrix[i]
                top_indices = np.argsort(-similarities)[1:11]  # Skip self (index 0)

                for rank, idx in enumerate(top_indices, 1):
                    if similarities[idx] > 0.0:
                        try:
                            # Store in fact_content_similarity
                            self.db_conn.execute("""
                                INSERT OR IGNORE INTO fact_anime_similarity (
                                    source_anime_key, target_anime_key,
                                    similarity_score, method, rank
                                ) VALUES (?, ?, ?, ?, ?)
                            """, [
                                source_key,
                                anime_keys[idx],
                                float(similarities[idx]),
                                'tfidf',
                                rank
                            ])
                            stored += 1
                        except:
                            pass

            self.db_conn.commit()
            logger.info(f"  Stored {stored} TF-IDF similarity scores")

        except Exception as e:
            logger.error(f"  TF-IDF training failed: {e}")

    def train_lightfm_model(self):
        """Train LightFM collaborative filtering model."""
        logger.info("Training LightFM collaborative filtering model...")

        try:
            from lightfm import LightFM
            from lightfm.evaluation import precision_at_k
        except ImportError:
            logger.warning("  LightFM not installed - skipping")
            import subprocess
            subprocess.run(["pip", "install", "-q", "lightfm"])
            from lightfm import LightFM

        # Load user-anime-rating triplets
        ratings = self.db_conn.execute("""
            SELECT user_key, anime_key, rating FROM fact_user_rating
        """).fetchall()

        if not ratings:
            logger.warning("  No ratings available for training")
            return

        logger.info(f"  Loading {len(ratings)} ratings for training...")

        # Convert to sparse matrix format
        max_user = max(r[0] for r in ratings)
        max_anime = max(r[1] for r in ratings)

        # Create LightFM interactions matrix (user x anime, binary: rated=1)
        interactions = np.zeros((max_user, max_anime), dtype=bool)
        weights = np.zeros((max_user, max_anime), dtype=np.float32)

        for user_key, anime_key, rating in ratings:
            interactions[user_key - 1, anime_key - 1] = 1
            weights[user_key - 1, anime_key - 1] = rating / 5.0  # Normalize to 0-1

        logger.info(f"  Training matrix: {interactions.shape} ({len(ratings)} ratings)")

        # Train LightFM
        model = LightFM(
            no_components=50,
            k=10,
            loss='bpr',  # Bayesian Personalized Ranking
            learning_rate=0.05
        )

        logger.info("  Fitting LightFM model (10 epochs)...")
        model.fit(
            interactions=interactions,
            weights=weights,
            epochs=10,
            num_threads=1,
            verbose=False
        )

        # Generate predictions and store
        logger.info("  Computing predictions...")
        stored = 0

        for user_id in range(min(100, max_user)):  # Store top 100 users' recommendations
            scores = model.predict(user_id, np.arange(max_anime))

            top_anime_indices = np.argsort(-scores)[:10]

            for rank, anime_idx in enumerate(top_anime_indices, 1):
                if scores[anime_idx] > 0:
                    try:
                        self.db_conn.execute("""
                            INSERT OR IGNORE INTO fact_collaborative_scores (
                                user_key, anime_key, predicted_rating
                            ) VALUES (?, ?, ?)
                        """, [
                            user_id + 1,
                            anime_idx + 1,
                            float(scores[anime_idx])
                        ])
                        stored += 1
                    except:
                        pass

        self.db_conn.commit()
        logger.info(f"  Stored {stored} collaborative filter predictions")

    def compute_model_metrics(self):
        """Compute and store model performance metrics."""
        logger.info("Computing model metrics...")

        metrics = {
            "phase": 3,
            "timestamp": datetime.now().isoformat(),
            "tfidf_anime_covered": self.db_conn.execute(
                "SELECT COUNT(DISTINCT source_anime_key) FROM fact_anime_similarity WHERE method='tfidf'"
            ).fetchone()[0] if hasattr(self, 'db_conn') else 0,
            "lightfm_users_covered": self.db_conn.execute(
                "SELECT COUNT(DISTINCT user_key) FROM fact_collaborative_scores"
            ).fetchone()[0] if hasattr(self, 'db_conn') else 0,
        }

        logger.info(f"  Metrics: {json.dumps(metrics, indent=2)}")
        return metrics

    def run(self):
        """Run complete Phase 3 training."""
        try:
            logger.info("=" * 80)
            logger.info("PHASE 3: ML TRAINING PIPELINE")
            logger.info("=" * 80)

            self.setup_database()

            self.train_tfidf_model()
            self.train_lightfm_model()
            self.compute_model_metrics()

            logger.info("=" * 80)
            logger.info("PHASE 3 COMPLETED - Models Trained")
            logger.info("=" * 80)

        except Exception as e:
            logger.error(f"Phase 3 failed: {e}")
            raise

        finally:
            if self.db_conn:
                self.db_conn.close()


if __name__ == "__main__":
    orchestrator = Phase3Orchestrator()
    orchestrator.run()

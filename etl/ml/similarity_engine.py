"""
Similarity Engine
=================
Computes anime similarity scores using TF-IDF and cosine similarity.
Stores results in fact_anime_similarity table for fast recommendations.

Algorithm:
- Combines genres and descriptions into feature vector
- Applies TF-IDF vectorization
- Computes pairwise cosine similarity
- Stores top N similar anime per anime
"""

import duckdb
import pandas as pd
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from typing import Optional
import logging

logger = logging.getLogger(__name__)


class SimilarityEngine:
    """
    Computes and stores anime similarity scores
    """

    def __init__(
        self,
        db_path: str,
        max_features: int = 1000,
        stop_words: str = 'english',
        min_similarity: float = 0.1,
        top_n: int = 50
    ):
        """
        Initialize similarity engine

        Args:
            db_path: Path to DuckDB database
            max_features: Maximum TF-IDF features
            stop_words: Stop words for TF-IDF
            min_similarity: Minimum similarity score to store
            top_n: Store only top N similar anime per anime
        """
        self.db_path = db_path
        self.max_features = max_features
        self.stop_words = stop_words
        self.min_similarity = min_similarity
        self.top_n = top_n

        self.tfidf = TfidfVectorizer(
            max_features=max_features,
            stop_words=stop_words,
            max_df=0.8,
            min_df=1
        )

        logger.info(f"Initialized SimilarityEngine (max_features={max_features}, top_n={top_n})")

    def compute_and_store(self):
        """
        Compute similarity scores for all anime and store in warehouse
        """
        logger.info("Starting similarity computation...")

        # Connect to warehouse
        conn = duckdb.connect(self.db_path)

        try:
            # Load anime data
            df = self._load_anime_data(conn)

            if len(df) < 2:
                logger.warning("Need at least 2 anime to compute similarity")
                return

            logger.info(f"Computing similarity for {len(df)} anime...")

            # Prepare features
            features = self._prepare_features(df)

            # Compute TF-IDF vectors
            tfidf_matrix = self.tfidf.fit_transform(features)

            logger.info(f"TF-IDF matrix shape: {tfidf_matrix.shape}")

            # Compute similarity matrix
            similarity_matrix = cosine_similarity(tfidf_matrix)

            logger.info(f"Similarity matrix computed: {similarity_matrix.shape}")

            # Store similarities
            self._store_similarities(conn, df, similarity_matrix)

            logger.info("Similarity computation complete")

        except Exception as e:
            logger.error(f"Similarity computation failed: {str(e)}")
            raise

        finally:
            conn.close()

    def _load_anime_data(self, conn: duckdb.DuckDBPyConnection) -> pd.DataFrame:
        """
        Load anime data with ENHANCED features from warehouse

        Args:
            conn: Database connection

        Returns:
            DataFrame with anime data including tags, studios, staff, etc.
        """
        logger.debug("Loading anime data from warehouse...")

        query = """
            SELECT
                anime_key,
                anime_id,
                title,
                description,
                genres,
                tags,
                studios,
                staff,
                characters,
                source,
                season,
                season_year
            FROM vw_anime_current
            WHERE description IS NOT NULL
            ORDER BY anime_key
        """

        df = conn.execute(query).fetchdf()

        logger.debug(f"Loaded {len(df)} anime records with enhanced features")
        return df

    def _prepare_features(self, df: pd.DataFrame) -> pd.Series:
        """
        Prepare WEIGHTED combined features for TF-IDF

        Feature weights:
        - Tags: 60% (most important - specific themes/elements)
        - Studios: 15% (production style)
        - Genres: 10% (broad categories)
        - Staff: 10% (director/writer influence)
        - Source: 3% (manga, light novel, etc.)
        - Description: 2% (subjective text)

        Args:
            df: DataFrame with anime data

        Returns:
            Series of weighted combined features
        """
        logger.debug("Preparing ENHANCED weighted features for TF-IDF...")

        def safe_str(val):
            """Convert to string, handle None/NaN"""
            if pd.isna(val) or val is None:
                return ''
            return str(val).replace('|', ' ')  # Convert pipe separators to spaces

        # Build weighted feature string
        features_list = []

        for idx, row in df.iterrows():
            feature_parts = []

            # Tags (60% weight) - repeat 6 times
            tags = safe_str(row.get('tags', ''))
            if tags:
                feature_parts.extend([tags] * 6)

            # Studios (15% weight) - repeat 3 times
            studios = safe_str(row.get('studios', ''))
            if studios:
                feature_parts.extend([studios] * 3)

            # Genres (10% weight) - repeat 2 times
            genres = safe_str(row.get('genres', ''))
            if genres:
                feature_parts.extend([genres] * 2)

            # Staff (10% weight) - repeat 2 times
            staff = safe_str(row.get('staff', ''))
            if staff:
                feature_parts.extend([staff] * 2)

            # Source (3% weight) - once
            source = safe_str(row.get('source', ''))
            if source:
                feature_parts.append(source)

            # Description (2% weight) - short excerpt only
            description = safe_str(row.get('description', ''))
            if description:
                # Take first 200 characters only
                feature_parts.append(description[:200])

            # Combine all parts
            combined = ' '.join(feature_parts)
            features_list.append(combined)

        features = pd.Series(features_list, index=df.index)

        logger.debug(f"Prepared {len(features)} enhanced feature vectors")
        logger.debug(f"Average feature length: {features.str.len().mean():.0f} characters")

        return features

    def _store_similarities(
        self,
        conn: duckdb.DuckDBPyConnection,
        df: pd.DataFrame,
        similarity_matrix: np.ndarray
    ):
        """
        Store similarity scores in warehouse

        Args:
            conn: Database connection
            df: DataFrame with anime data
            similarity_matrix: Pairwise similarity matrix
        """
        logger.info("Storing similarity scores...")

        # Clear existing similarities
        conn.execute("DELETE FROM fact_anime_similarity")

        # Extract anime_keys
        anime_keys = df['anime_key'].values

        # Store similarities
        stored_count = 0

        for i, anime_key_1 in enumerate(anime_keys):
            # Get similarity scores for this anime
            scores = similarity_matrix[i]

            # Get indices of top N similar anime (excluding self)
            # Sort indices by score descending
            similar_indices = np.argsort(-scores)

            # Store top N (excluding self)
            for j in similar_indices:
                if i == j:  # Skip self-similarity
                    continue

                anime_key_2 = anime_keys[j]
                score = float(scores[j])

                # Only store if above threshold
                if score < self.min_similarity:
                    break  # Scores are sorted, so we can break

                # Only store top N
                if stored_count >= self.top_n * len(anime_keys):
                    break

                # Insert similarity
                try:
                    conn.execute("""
                        INSERT INTO fact_anime_similarity (
                            anime_key_1, anime_key_2, similarity_score,
                            method, computed_at
                        ) VALUES (?, ?, ?, 'tfidf_cosine', CURRENT_TIMESTAMP)
                    """, [int(anime_key_1), int(anime_key_2), score])

                    stored_count += 1

                except Exception as e:
                    logger.warning(f"Failed to store similarity ({anime_key_1}, {anime_key_2}): {e}")

            # Log progress every 10 anime
            if (i + 1) % 10 == 0:
                logger.debug(f"Processed {i + 1}/{len(anime_keys)} anime")

        logger.info(f"Stored {stored_count} similarity scores")

    def get_recommendations(
        self,
        conn: duckdb.DuckDBPyConnection,
        anime_id: int,
        n: int = 5
    ) -> pd.DataFrame:
        """
        Get top N recommendations for an anime

        Args:
            conn: Database connection
            anime_id: AniList anime ID
            n: Number of recommendations

        Returns:
            DataFrame with recommended anime
        """
        query = """
            SELECT
                a2.anime_id,
                a2.title,
                a2.description,
                a2.averageScore,
                a2.genres,
                s.similarity_score
            FROM vw_anime_current a1
            JOIN fact_anime_similarity s ON a1.anime_key = s.anime_key_1
            JOIN vw_anime_current a2 ON s.anime_key_2 = a2.anime_key
            WHERE a1.anime_id = ?
            ORDER BY s.similarity_score DESC
            LIMIT ?
        """

        return conn.execute(query, [anime_id, n]).fetchdf()


def compute_similarities(db_path: str):
    """
    Convenience function to compute similarities

    Args:
        db_path: Path to DuckDB database
    """
    engine = SimilarityEngine(db_path)
    engine.compute_and_store()


if __name__ == '__main__':
    # Test similarity computation
    logging.basicConfig(level=logging.INFO)

    db_path = 'warehouse/anime_dw.duckdb'

    engine = SimilarityEngine(db_path)
    engine.compute_and_store()

    # Test recommendations
    conn = duckdb.connect(db_path)

    # Get anime list
    anime_list = conn.execute("SELECT anime_id, title FROM vw_anime_current LIMIT 3").fetchall()

    print("\nTesting recommendations:")
    for anime_id, title in anime_list:
        print(f"\n{title} (ID: {anime_id}):")
        recommendations = engine.get_recommendations(conn, anime_id, n=5)
        for _, rec in recommendations.iterrows():
            print(f"  - {rec['title']}: {rec['similarity_score']:.4f}")

    conn.close()

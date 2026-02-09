"""
Warehouse Database Connection
==============================
Provides connection management and query helpers for the DuckDB warehouse
"""

import duckdb
import pandas as pd
from typing import List, Dict, Any, Optional
from pathlib import Path
import logging

logger = logging.getLogger(__name__)


class WarehouseConnection:
    """
    Manages connection to the anime data warehouse
    Provides high-level query methods for the Streamlit app
    """

    def __init__(self, db_path: str = None):
        """
        Initialize warehouse connection

        Args:
            db_path: Path to DuckDB database file. If None, uses default.
        """
        if db_path is None:
            # Default path
            db_path = Path(__file__).parent.parent.parent / 'warehouse' / 'anime_full_phase1.duckdb'

        self.db_path = str(db_path)
        self.conn = None

        logger.info(f"Initialized WarehouseConnection for {self.db_path}")

    def connect(self):
        """Open database connection"""
        if self.conn is None:
            self.conn = duckdb.connect(self.db_path, read_only=True)
            logger.debug("Connected to warehouse")

    def close(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
            self.conn = None
            logger.debug("Closed warehouse connection")

    def __enter__(self):
        """Context manager entry"""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.close()

    def get_all_anime(self) -> pd.DataFrame:
        """
        Get all anime with their latest metrics

        Returns:
            DataFrame with all anime data
        """
        self.connect()

        query = """
            SELECT
                anime_id as id,
                title,
                description,
                siteUrl,
                coverImage,
                genres,
                format,
                averageScore,
                popularity,
                episodes,
                tags,
                studios,
                source,
                season,
                season_year
            FROM vw_anime_current
            ORDER BY popularity DESC
        """

        df = self.conn.execute(query).fetchdf()
        logger.debug(f"Retrieved {len(df)} anime from warehouse")

        return df

    def get_anime_by_id(self, anime_id: int) -> Optional[pd.Series]:
        """
        Get a specific anime by ID

        Args:
            anime_id: AniList anime ID

        Returns:
            Series with anime data, or None if not found
        """
        self.connect()

        query = """
            SELECT
                anime_id as id,
                title,
                description,
                siteUrl,
                coverImage,
                genres,
                format,
                averageScore,
                popularity,
                episodes,
                tags,
                studios,
                source,
                season,
                season_year
            FROM vw_anime_current
            WHERE anime_id = ?
        """

        result = self.conn.execute(query, [anime_id]).fetchdf()

        if len(result) > 0:
            return result.iloc[0]
        return None

    def get_top_rated(self, limit: int = 20) -> pd.DataFrame:
        """
        Get top rated anime

        Args:
            limit: Number of anime to return

        Returns:
            DataFrame with top rated anime
        """
        self.connect()

        query = """
            SELECT
                anime_id as id,
                title,
                description,
                siteUrl,
                coverImage,
                genres,
                format,
                averageScore,
                popularity,
                episodes
            FROM vw_anime_current
            WHERE averageScore IS NOT NULL
            ORDER BY averageScore DESC, popularity DESC
            LIMIT ?
        """

        df = self.conn.execute(query, [limit]).fetchdf()
        logger.debug(f"Retrieved {len(df)} top rated anime")

        return df

    def get_most_popular(self, limit: int = 20) -> pd.DataFrame:
        """
        Get most popular anime

        Args:
            limit: Number of anime to return

        Returns:
            DataFrame with most popular anime
        """
        self.connect()

        query = """
            SELECT
                anime_id as id,
                title,
                description,
                siteUrl,
                coverImage,
                genres,
                format,
                averageScore,
                popularity,
                episodes
            FROM vw_anime_current
            ORDER BY popularity DESC
            LIMIT ?
        """

        df = self.conn.execute(query, [limit]).fetchdf()
        logger.debug(f"Retrieved {len(df)} most popular anime")

        return df

    def get_recommendations(self, anime_title: str, limit: int = 10) -> pd.DataFrame:
        """
        Get pre-computed recommendations for an anime using similarity scores

        Args:
            anime_title: Title of the anime to get recommendations for
            limit: Number of recommendations to return

        Returns:
            DataFrame with recommended anime and similarity scores
        """
        self.connect()

        # First try exact match, then fall back to LIKE
        exact = self.conn.execute(
            "SELECT anime_key FROM vw_anime_current WHERE title = ?",
            [anime_title]
        ).fetchone()

        if exact:
            where_clause = "a1.anime_key = ?"
            param = exact[0]
        else:
            where_clause = "a1.title LIKE ?"
            param = f"%{anime_title}%"

        query = f"""
            SELECT DISTINCT
                a2.anime_id as id,
                a2.title,
                a2.description,
                a2.siteUrl,
                a2.coverImage,
                a2.genres,
                a2.format,
                a2.averageScore,
                a2.popularity,
                a2.episodes,
                s.similarity_score
            FROM vw_anime_current a1
            JOIN fact_anime_similarity s ON a1.anime_key = s.anime_key_1
            JOIN vw_anime_current a2 ON s.anime_key_2 = a2.anime_key
            WHERE {where_clause}
            ORDER BY s.similarity_score DESC
            LIMIT ?
        """

        df = self.conn.execute(query, [param, limit]).fetchdf()
        logger.debug(f"Retrieved {len(df)} recommendations for '{anime_title}'")

        return df

    def filter_anime(
        self,
        genres: List[str] = None,
        min_score: float = None,
        format_type: str = None,
        sort_by: str = 'popularity'
    ) -> pd.DataFrame:
        """
        Filter anime by various criteria

        Args:
            genres: List of genres to filter by (any match)
            min_score: Minimum average score
            format_type: Format type (TV, Movie, etc.)
            sort_by: Sort field ('popularity', 'score', 'title')

        Returns:
            DataFrame with filtered anime
        """
        self.connect()

        # Build WHERE clauses
        where_clauses = []
        params = []

        if genres:
            # Match any of the provided genres
            genre_conditions = []
            for genre in genres:
                genre_conditions.append("genres LIKE ?")
                params.append(f"%{genre}%")
            where_clauses.append(f"({' OR '.join(genre_conditions)})")

        if min_score is not None:
            where_clauses.append("averageScore >= ?")
            params.append(min_score)

        if format_type:
            where_clauses.append("format = ?")
            params.append(format_type)

        # Build WHERE clause
        where_sql = ""
        if where_clauses:
            where_sql = "WHERE " + " AND ".join(where_clauses)

        # Build ORDER BY clause
        order_by_map = {
            'popularity': 'popularity DESC',
            'score': 'averageScore DESC, popularity DESC',
            'title': 'title ASC'
        }
        order_by_sql = order_by_map.get(sort_by, 'popularity DESC')

        query = f"""
            SELECT
                anime_id as id,
                title,
                description,
                siteUrl,
                coverImage,
                genres,
                format,
                averageScore,
                popularity,
                episodes
            FROM vw_anime_current
            {where_sql}
            ORDER BY {order_by_sql}
        """

        df = self.conn.execute(query, params).fetchdf()
        logger.debug(f"Filtered {len(df)} anime")

        return df

    def get_related_anime(self, anime_id: int) -> List[int]:
        """
        Get all anime IDs related to a particular anime (sequels, prequels, parents, etc.)
        Use this to exclude them from recommendations when the user has already selected anime.

        Args:
            anime_id: AniList anime ID

        Returns:
            List of related anime IDs
        """
        self.connect()

        query = """
            SELECT target_anime_id
            FROM bridge_anime_relations
            WHERE source_anime_id = ?
            UNION
            SELECT source_anime_id
            FROM bridge_anime_relations
            WHERE target_anime_id = ?
        """

        results = self.conn.execute(query, [anime_id, anime_id]).fetchall()
        related_ids = [r[0] for r in results]

        logger.debug(f"Retrieved {len(related_ids)} related anime for ID {anime_id}")
        return related_ids

    def get_statistics(self) -> Dict[str, Any]:
        """
        Get warehouse statistics

        Returns:
            Dictionary with statistics
        """
        self.connect()

        stats = {}

        # Total anime
        stats['total_anime'] = self.conn.execute(
            "SELECT COUNT(*) FROM vw_anime_current"
        ).fetchone()[0]

        # Average score
        stats['avg_score'] = self.conn.execute(
            "SELECT AVG(averageScore) FROM vw_anime_current WHERE averageScore IS NOT NULL"
        ).fetchone()[0]

        # Total similarity scores
        stats['similarity_scores'] = self.conn.execute(
            "SELECT COUNT(*) FROM fact_anime_similarity"
        ).fetchone()[0]

        # Top genres
        top_genres = self.conn.execute("""
            SELECT genre_name, anime_count
            FROM vw_genre_popularity
            ORDER BY anime_count DESC
            LIMIT 5
        """).fetchall()
        stats['top_genres'] = [{'genre': g[0], 'count': g[1]} for g in top_genres]

        logger.debug(f"Retrieved warehouse statistics: {stats['total_anime']} anime")

        return stats


if __name__ == '__main__':
    # Test connection
    logging.basicConfig(level=logging.DEBUG)

    with WarehouseConnection() as db:
        print("Testing WarehouseConnection...")

        # Test get all anime
        all_anime = db.get_all_anime()
        print(f"Total anime: {len(all_anime)}")

        # Test top rated
        top_rated = db.get_top_rated(5)
        print(f"\nTop 5 rated:")
        for _, anime in top_rated.iterrows():
            print(f"  - {anime['title']}: {anime['averageScore']}")

        # Test recommendations
        recs = db.get_recommendations("Attack on Titan", 5)
        print(f"\nRecommendations for Attack on Titan:")
        for _, anime in recs.iterrows():
            print(f"  - {anime['title']}: {anime['similarity_score']:.4f}")

        # Test statistics
        stats = db.get_statistics()
        print(f"\nStatistics:")
        print(f"  Total anime: {stats['total_anime']}")
        print(f"  Avg score: {stats['avg_score']:.2f}")
        print(f"  Similarity scores: {stats['similarity_scores']}")

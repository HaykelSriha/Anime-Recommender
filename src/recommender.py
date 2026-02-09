import re
import pandas as pd
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
try:
    from .database.db_connector import WarehouseConnection
except ImportError:
    from database.db_connector import WarehouseConnection
import logging

logger = logging.getLogger(__name__)

# Season/sequel patterns for filtering out same-series recommendations
_SEASON_PATTERNS = [
    r'\s+Season\s+\d+',
    r'\s+S\d+',
    r'\s+Part\s+\d+',
    r'\s+\(Season\s+\d+\)',
    r'\s+The\s+Final\s+Season',
    r'\s+Final\s+Season',
    r'\s+\d+(st|nd|rd|th)\s+Season',
    r'\s+OVA\b.*',
    r'\s+Movie\b.*',
    r'\s+Specials?\b.*',
    r'\s+Chronicle\b.*',
]


def _get_base_series_name(title):
    """Extract base series name by removing season/sequel indicators.

    Uses two strategies and returns the shorter (more aggressive) result:
    1. Strip known English season patterns
    2. Take everything before the first colon (catches Japanese subtitles)
    """
    # Strategy 1: regex strip
    base = title
    for pattern in _SEASON_PATTERNS:
        base = re.sub(pattern, '', base, flags=re.IGNORECASE)
    base = base.strip()

    # Strategy 2: colon split (catches Japanese naming like "Title: Season Name")
    if ':' in title:
        colon_base = title.split(':')[0].strip()
        # Use the shorter result (more aggressive grouping)
        if len(colon_base) >= 3:  # avoid overly short names
            base = min(base, colon_base, key=len)

    return base


def _filter_and_dedup(df, excluded_bases, n, sim_col='similarity_score'):
    """Filter out excluded series and deduplicate results by series name.

    Keeps only the highest-similarity entry per series.

    Args:
        df: DataFrame of recommendations (must have 'title' and sim_col columns)
        excluded_bases: Set of lowercase base series names to exclude entirely
        n: Max results to return
        sim_col: Column name for similarity score
    """
    seen_series = set()
    keep_indices = []

    for idx, row in df.iterrows():
        base = _get_base_series_name(row['title']).lower()

        # Skip if same series as an input anime
        if any(base.startswith(ex) or ex.startswith(base) for ex in excluded_bases):
            continue

        # Deduplicate: keep only the first (highest similarity) per series
        if base in seen_series:
            continue
        seen_series.add(base)
        keep_indices.append(idx)

        if len(keep_indices) >= n:
            break

    return df.loc[keep_indices]


class AnimeRecommender:
    def __init__(self, data_path=None, use_warehouse=True):
        """
        Initialize anime recommender

        Args:
            data_path: Path to CSV file (for backward compatibility)
            use_warehouse: If True, use DuckDB warehouse (recommended)
        """
        self.use_warehouse = use_warehouse

        if use_warehouse:
            # Use warehouse - much faster with pre-computed similarities!
            self.db = WarehouseConnection()
            self.df = self.db.get_all_anime()
            self.tfidf_matrix = None  # Not needed - pre-computed in warehouse
            self.similarity_matrix = None  # Not needed - pre-computed in warehouse
            logger.info(f"Loaded {len(self.df)} anime from warehouse")
        else:
            # Legacy CSV mode
            self.db = None
            self.df = pd.read_csv(data_path)
            self.prepare_features()
            logger.info(f"Loaded {len(self.df)} anime from CSV")

    def prepare_features(self):
        """Prepare features for recommendation (CSV mode only)"""
        # Combine genres and description for similarity calculation
        self.df['combined_features'] = (
            self.df['genres'].fillna('').astype(str) + ' ' +
            self.df['description'].fillna('').astype(str)
        )

        # Create TF-IDF vectors
        self.tfidf = TfidfVectorizer(max_features=1000, stop_words='english')
        self.tfidf_matrix = self.tfidf.fit_transform(self.df['combined_features'])

        # Compute similarity matrix
        self.similarity_matrix = cosine_similarity(self.tfidf_matrix)
    
    def get_recommendations(self, anime_title, n_recommendations=5):
        """
        Get anime recommendations based on a selected anime

        Uses pre-computed similarities from warehouse (instant!)
        or computes on-the-fly for CSV mode
        """
        if self.use_warehouse:
            # Fetch extra candidates so we still have enough after filtering
            recommendations = self.db.get_recommendations(anime_title, n_recommendations * 3)

            if len(recommendations) == 0:
                logger.warning(f"No recommendations found for '{anime_title}'")
                return None

            # Filter out same-series seasons/sequels + deduplicate by series
            input_base = _get_base_series_name(anime_title).lower()
            recommendations = _filter_and_dedup(recommendations, {input_base}, n_recommendations)

            return recommendations
        else:
            # Legacy CSV mode - compute on-the-fly
            matching_indices = self.df[self.df['title'].str.contains(anime_title, case=False, na=False)].index

            if len(matching_indices) == 0:
                return None

            idx = matching_indices[0]

            # Get similarity scores
            sim_scores = list(enumerate(self.similarity_matrix[idx]))
            sim_scores = sorted(sim_scores, key=lambda x: x[1], reverse=True)

            # Get top recommendations (excluding the anime itself)
            sim_scores = sim_scores[1:n_recommendations + 1]
            anime_indices = [i[0] for i in sim_scores]

            return self.df.iloc[anime_indices]
    
    def filter_by_genre(self, genre):
        """Filter anime by genre"""
        if self.use_warehouse:
            # Use optimized warehouse query
            return self.db.filter_anime(genres=[genre])
        else:
            # Legacy CSV mode
            return self.df[self.df['genres'].str.contains(genre, case=False, na=False)]

    def get_top_rated(self, n=10):
        """Get top rated anime"""
        if self.use_warehouse:
            # Use optimized warehouse query
            return self.db.get_top_rated(n)
        else:
            # Legacy CSV mode
            return self.df.nlargest(n, 'averageScore')

    def get_most_popular(self, n=10):
        """Get most popular anime"""
        if self.use_warehouse:
            # Use optimized warehouse query
            return self.db.get_most_popular(n)
        else:
            # Legacy CSV mode
            return self.df.nlargest(n, 'popularity')

    def get_all_genres(self):
        """Get unique genres"""
        genres = set()
        for genre_str in self.df['genres'].dropna():
            genres.update(genre_str.split('|'))
        return sorted(list(genres))

    def get_stats(self):
        """Get warehouse statistics (warehouse mode only)"""
        if self.use_warehouse:
            return self.db.get_statistics()
        else:
            return {
                'total_anime': len(self.df),
                'avg_score': self.df['averageScore'].mean(),
                'top_genres': []
            }

    def get_multi_anime_recommendations(self, anime_titles, n_recommendations=10):
        """
        Get recommendations based on MULTIPLE anime (find anime similar to ALL of them)

        This is the CORE feature: "I like these 5 anime, find me similar ones!"

        Algorithm:
        - For each candidate anime, compute average similarity to all favorites
        - Rank by average similarity score
        - Return top N matches

        Args:
            anime_titles: List of anime titles user likes
            n_recommendations: Number of recommendations to return

        Returns:
            DataFrame with recommended anime and aggregated similarity scores
        """
        if not anime_titles or len(anime_titles) == 0:
            logger.warning("No anime titles provided for multi-anime recommendations")
            return pd.DataFrame()

        if self.use_warehouse:
            # Use warehouse with pre-computed similarities
            return self._get_multi_recommendations_warehouse(anime_titles, n_recommendations)
        else:
            # Legacy CSV mode
            return self._get_multi_recommendations_csv(anime_titles, n_recommendations)

    def _get_multi_recommendations_warehouse(self, anime_titles, n_recommendations):
        """Get multi-anime recommendations using warehouse"""
        # Build set of base series names for favorites
        favorite_base_names = set()
        for title in anime_titles:
            favorite_base_names.add(_get_base_series_name(title).lower())

        # Collect all recommendations for each anime
        all_recommendations = {}

        for title in anime_titles:
            recs = self.db.get_recommendations(title, limit=100)  # Get more candidates

            if len(recs) > 0:
                for _, anime in recs.iterrows():
                    anime_id = anime['id']
                    similarity = anime['similarity_score']

                    if anime_id not in all_recommendations:
                        all_recommendations[anime_id] = {
                            'anime': anime,
                            'similarities': [],
                            'matched_with': []
                        }

                    all_recommendations[anime_id]['similarities'].append(similarity)
                    all_recommendations[anime_id]['matched_with'].append(title)

        # Compute average similarity for each candidate
        results = []
        for anime_id, data in all_recommendations.items():
            avg_similarity = np.mean(data['similarities'])
            anime_data = data['anime'].to_dict()
            anime_data['avg_similarity'] = avg_similarity
            anime_data['match_count'] = len(data['similarities'])
            if 'similarity_score' in anime_data:
                del anime_data['similarity_score']
            results.append(anime_data)

        if not results:
            return pd.DataFrame()

        results_df = pd.DataFrame(results)
        results_df = results_df.sort_values(
            by=['avg_similarity', 'match_count'],
            ascending=[False, False]
        )

        # Exclude input anime and deduplicate by series
        input_anime_ids = set()
        for title in anime_titles:
            matching = self.df[self.df['title'].str.lower() == title.lower()]
            if len(matching) == 0:
                matching = self.df[self.df['title'].str.contains(title, case=False, na=False)]
            if len(matching) > 0:
                input_anime_ids.add(matching.iloc[0]['id'])

        results_df = results_df[~results_df['id'].isin(input_anime_ids)]
        results_df = _filter_and_dedup(results_df, favorite_base_names, n_recommendations, sim_col='avg_similarity')

        logger.info(f"Found {len(results_df)} recommendations for {len(anime_titles)} favorites")
        return results_df

    def _get_multi_recommendations_csv(self, anime_titles, n_recommendations):
        """Get multi-anime recommendations using CSV (legacy mode)"""
        # Find indices for all input anime
        input_indices = []
        for title in anime_titles:
            matching = self.df[self.df['title'].str.contains(title, case=False, na=False)]
            if len(matching) > 0:
                input_indices.append(matching.index[0])

        if len(input_indices) == 0:
            return pd.DataFrame()

        # Compute average similarity for each candidate anime
        avg_similarities = np.mean([self.similarity_matrix[idx] for idx in input_indices], axis=0)

        # Get top recommendations (excluding input anime)
        sim_scores = list(enumerate(avg_similarities))
        sim_scores = sorted(sim_scores, key=lambda x: x[1], reverse=True)

        # Filter out input anime
        sim_scores = [(i, score) for i, score in sim_scores if i not in input_indices]

        # Get top N
        sim_scores = sim_scores[:n_recommendations]
        anime_indices = [i[0] for i in sim_scores]

        results = self.df.iloc[anime_indices].copy()
        results['avg_similarity'] = [score for _, score in sim_scores]

        return results

"""
Anime Data Transformer
======================
Transforms raw anime data from AniList API into warehouse-ready format.

Includes:
- Schema validation
- Data cleansing (HTML removal, normalization)
- Data enrichment (percentiles, rankings)
- Quality checks
"""

import re
import html
from typing import List, Dict, Any, Tuple
import pandas as pd
import logging
from datetime import datetime, date

logger = logging.getLogger(__name__)


class AnimeTransformer:
    """
    Transforms raw anime data into warehouse schema
    """

    def __init__(
        self,
        remove_html: bool = True,
        normalize_genres: bool = True,
        calculate_metrics: bool = True
    ):
        """
        Initialize transformer

        Args:
            remove_html: Whether to remove HTML tags from descriptions
            normalize_genres: Whether to normalize genre names
            calculate_metrics: Whether to calculate derived metrics
        """
        self.remove_html = remove_html
        self.normalize_genres = normalize_genres
        self.calculate_metrics = calculate_metrics

        logger.info("Initialized AnimeTransformer")

    def transform(self, raw_data: List[Dict[str, Any]]) -> Tuple[pd.DataFrame, List[str]]:
        """
        Transform raw anime data

        Args:
            raw_data: List of raw anime dictionaries from API

        Returns:
            Tuple of (transformed DataFrame, list of error messages)
        """
        logger.info(f"Transforming {len(raw_data)} anime records")

        errors = []

        # Convert to DataFrame
        df = pd.DataFrame(raw_data)

        # Validate schema
        validation_errors = self._validate_schema(df)
        if validation_errors:
            errors.extend(validation_errors)
            logger.warning(f"Schema validation found {len(validation_errors)} issues")

        # Cleanse data
        df = self._cleanse_data(df)

        # Normalize genres
        if self.normalize_genres:
            df = self._normalize_genres(df)

        # Calculate derived metrics
        if self.calculate_metrics:
            df = self._calculate_metrics(df)

        # Add metadata
        df['extracted_at'] = datetime.now()
        df['snapshot_date'] = date.today()

        logger.info(f"Transformation complete: {len(df)} records processed")

        return df, errors

    def _validate_schema(self, df: pd.DataFrame) -> List[str]:
        """
        Validate that DataFrame has required fields and correct types

        Args:
            df: DataFrame to validate

        Returns:
            List of validation error messages
        """
        errors = []

        # Required fields
        required_fields = ['id', 'title']

        for field in required_fields:
            if field not in df.columns:
                errors.append(f"Missing required field: {field}")

        # Check for null IDs
        if 'id' in df.columns:
            null_ids = df['id'].isnull().sum()
            if null_ids > 0:
                errors.append(f"Found {null_ids} records with null ID")

        # Check for duplicate IDs
        if 'id' in df.columns:
            duplicates = df['id'].duplicated().sum()
            if duplicates > 0:
                errors.append(f"Found {duplicates} duplicate IDs")

        # Check averageScore range
        if 'averageScore' in df.columns:
            out_of_range = df[
                (df['averageScore'].notna()) &
                ((df['averageScore'] < 0) | (df['averageScore'] > 100))
            ].shape[0]

            if out_of_range > 0:
                errors.append(f"Found {out_of_range} records with averageScore out of range (0-100)")

        # Check popularity
        if 'popularity' in df.columns:
            negative_pop = df[
                (df['popularity'].notna()) & (df['popularity'] < 0)
            ].shape[0]

            if negative_pop > 0:
                errors.append(f"Found {negative_pop} records with negative popularity")

        return errors

    def _cleanse_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Cleanse data (remove HTML, handle nulls, etc.)

        Args:
            df: DataFrame to cleanse

        Returns:
            Cleansed DataFrame
        """
        logger.debug("Cleansing data...")

        df = df.copy()

        # Remove HTML from descriptions
        if self.remove_html and 'description' in df.columns:
            df['description'] = df['description'].apply(self._remove_html_tags)

        # Process nested structures (tags, studios, staff, characters)
        if 'tags' in df.columns:
            df['tags_processed'] = df['tags'].apply(self._extract_tags)

        if 'studios' in df.columns:
            df['studios_processed'] = df['studios'].apply(self._extract_studios)

        if 'staff' in df.columns:
            df['staff_processed'] = df['staff'].apply(self._extract_staff)

        if 'characters' in df.columns:
            df['characters_processed'] = df['characters'].apply(self._extract_characters)

        if 'relations' in df.columns:
            # Extract parent and root IDs
            relations_extracted = df['relations'].apply(self._extract_relations)
            df['parent_anime_id'] = relations_extracted.apply(lambda x: x[0])
            df['series_root_id'] = relations_extracted.apply(lambda x: x[1])
            df['relations_processed'] = df['relations']  # Keep original for reference

        # Standardize null values
        df = df.where(pd.notnull(df), None)

        # Trim whitespace from strings
        string_columns = df.select_dtypes(include=['object']).columns
        for col in string_columns:
            if col in df.columns:
                df[col] = df[col].apply(lambda x: x.strip() if isinstance(x, str) else x)

        # Ensure numeric types
        if 'id' in df.columns:
            df['id'] = pd.to_numeric(df['id'], errors='coerce')

        if 'averageScore' in df.columns:
            df['averageScore'] = pd.to_numeric(df['averageScore'], errors='coerce')

        if 'popularity' in df.columns:
            df['popularity'] = pd.to_numeric(df['popularity'], errors='coerce')

        if 'episodes' in df.columns:
            df['episodes'] = pd.to_numeric(df['episodes'], errors='coerce')

        logger.debug("Data cleansing complete")
        return df

    def _remove_html_tags(self, text: Any) -> Any:
        """
        Remove HTML tags and entities from text

        Args:
            text: Text to clean

        Returns:
            Cleaned text
        """
        if not isinstance(text, str) or not text:
            return text

        # Remove HTML tags
        text = re.sub(r'<[^>]+>', '', text)

        # Decode HTML entities
        text = html.unescape(text)

        # Remove excessive whitespace
        text = re.sub(r'\s+', ' ', text).strip()

        return text

    def _normalize_genres(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Normalize genre names (consistent casing, remove duplicates)

        Args:
            df: DataFrame with genres column

        Returns:
            DataFrame with normalized genres
        """
        if 'genres' not in df.columns:
            return df

        logger.debug("Normalizing genres...")

        df = df.copy()

        def normalize_genre_string(genre_str):
            if not isinstance(genre_str, str) or not genre_str:
                return genre_str

            # Split by pipe
            genres = genre_str.split('|')

            # Normalize each genre
            normalized = []
            for genre in genres:
                genre = genre.strip()

                # Standardize common variations
                replacements = {
                    'Sci-fi': 'Sci-Fi',
                    'SciFi': 'Sci-Fi',
                    'Slice Of Life': 'Slice of Life',
                    'Slice-of-Life': 'Slice of Life',
                }

                genre = replacements.get(genre, genre)

                if genre and genre not in normalized:
                    normalized.append(genre)

            return '|'.join(normalized)

        df['genres'] = df['genres'].apply(normalize_genre_string)

        logger.debug("Genre normalization complete")
        return df

    def _calculate_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate derived metrics (percentiles, rankings)

        Args:
            df: DataFrame to calculate metrics for

        Returns:
            DataFrame with additional calculated metrics
        """
        logger.debug("Calculating derived metrics...")

        df = df.copy()

        # Calculate score percentile
        if 'averageScore' in df.columns:
            df['score_percentile'] = df['averageScore'].rank(pct=True) * 100

        # Calculate popularity rank
        if 'popularity' in df.columns:
            df['popularity_rank'] = df['popularity'].rank(ascending=False, method='min')

        logger.debug("Metric calculation complete")
        return df

    def _extract_tags(self, tags_list: Any) -> str:
        """
        Extract and format tags from nested structure

        Args:
            tags_list: List of tag dictionaries from API

        Returns:
            Pipe-separated string of high-rank tags
        """
        if not tags_list or not isinstance(tags_list, list):
            return ''

        # Filter high-rank tags (rank > 60) and exclude spoilers
        high_tags = [
            tag['name'] for tag in tags_list
            if tag.get('rank', 0) > 60 and not tag.get('isMediaSpoiler', False)
        ]

        return '|'.join(high_tags)

    def _extract_studios(self, studios_dict: Any) -> str:
        """
        Extract studio names from nested structure

        Args:
            studios_dict: Studios dictionary from API

        Returns:
            Pipe-separated string of animation studio names
        """
        if not studios_dict or not isinstance(studios_dict, dict):
            return ''

        nodes = studios_dict.get('nodes', [])
        if not nodes:
            return ''

        # Filter for animation studios only
        studio_names = [
            node['name'] for node in nodes
            if node.get('isAnimationStudio', False)
        ]

        return '|'.join(studio_names)

    def _extract_staff(self, staff_dict: Any) -> str:
        """
        Extract key staff (director, writer) from nested structure

        Args:
            staff_dict: Staff dictionary from API

        Returns:
            Pipe-separated string of "role:name" pairs
        """
        if not staff_dict or not isinstance(staff_dict, dict):
            return ''

        edges = staff_dict.get('edges', [])
        if not edges:
            return ''

        # Extract role and name
        staff_list = []
        for edge in edges:
            role = edge.get('role', '')
            name = edge.get('node', {}).get('name', {}).get('full', '')
            if role and name:
                staff_list.append(f"{role}:{name}")

        return '|'.join(staff_list[:5])  # Limit to top 5

    def _extract_characters(self, characters_dict: Any) -> str:
        """
        Extract main characters from nested structure

        Args:
            characters_dict: Characters dictionary from API

        Returns:
            Pipe-separated string of character names (MAIN role only)
        """
        if not characters_dict or not isinstance(characters_dict, dict):
            return ''

        edges = characters_dict.get('edges', [])
        if not edges:
            return ''

        # Extract only MAIN characters
        main_chars = [
            edge['node']['name']['full']
            for edge in edges
            if edge.get('role') == 'MAIN' and edge.get('node', {}).get('name', {}).get('full')
        ]

        return '|'.join(main_chars[:10])  # Limit to top 10

    def _extract_relations(self, relations_str: Any) -> Tuple[str, int, int]:
        """
        Extract parent and root series IDs from relations JSON string.
        Identifies SEQUEL/PREQUEL relationships to group seasons together.

        Args:
            relations_str: Relations JSON string from API

        Returns:
            Tuple of (parent_anime_id, series_root_id)
        """
        if not relations_str or not isinstance(relations_str, str):
            return None, None

        try:
            import ast
            relations_list = ast.literal_eval(relations_str)
        except (ValueError, SyntaxError):
            return None, None

        if not isinstance(relations_list, list):
            return None, None

        parent_anime_id = None
        series_root_id = None

        # Look for parent or prequel to establish series hierarchy
        for rel in relations_list:
            rel_type = rel.get('relationType', '').upper()
            anime_id = rel.get('animeId')

            # PARENT is the original series
            if rel_type == 'PARENT' and not parent_anime_id:
                parent_anime_id = anime_id
                series_root_id = anime_id
                break

            # PREQUEL chains backwards to find root
            elif rel_type == 'PREQUEL' and not parent_anime_id:
                parent_anime_id = anime_id
                # For prequels, we don't set root yet (might chain further)

        return parent_anime_id, series_root_id

    def prepare_for_warehouse(self, df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        """
        Split transformed data into warehouse tables

        Args:
            df: Transformed DataFrame

        Returns:
            Dictionary of DataFrames for each warehouse table
        """
        logger.info("Preparing data for warehouse loading...")

        result = {}

        # Prepare anime dimensions
        anime_cols = ['id', 'title', 'description', 'siteUrl', 'coverImage']
        result['dim_anime'] = df[[col for col in anime_cols if col in df.columns]].copy()

        # Prepare formats
        if 'format' in df.columns:
            result['dim_format'] = pd.DataFrame({
                'format_name': df['format'].dropna().unique()
            })

        # Prepare genres (exploded)
        if 'genres' in df.columns:
            genres = set()
            for genre_str in df['genres'].dropna():
                if isinstance(genre_str, str):
                    genres.update(genre_str.split('|'))

            result['dim_genre'] = pd.DataFrame({
                'genre_name': sorted(genres)
            })

        # Prepare metrics facts
        metric_cols = [
            'id', 'averageScore', 'popularity', 'episodes',
            'format', 'score_percentile', 'popularity_rank', 'snapshot_date'
        ]
        result['fact_metrics'] = df[[col for col in metric_cols if col in df.columns]].copy()

        # Prepare genre bridge
        if 'genres' in df.columns:
            bridge_data = []
            for _, row in df.iterrows():
                anime_id = row['id']
                genre_str = row.get('genres')

                if isinstance(genre_str, str) and genre_str:
                    genres = genre_str.split('|')
                    for genre in genres:
                        bridge_data.append({
                            'anime_id': anime_id,
                            'genre_name': genre.strip()
                        })

            result['bridge_anime_genre'] = pd.DataFrame(bridge_data)

        logger.info(f"Prepared {len(result)} warehouse tables")
        return result


# Convenience function
def transform_anime_data(raw_data: List[Dict[str, Any]]) -> pd.DataFrame:
    """
    Convenience function to transform anime data

    Args:
        raw_data: List of raw anime dictionaries

    Returns:
        Transformed DataFrame
    """
    transformer = AnimeTransformer()
    df, errors = transformer.transform(raw_data)

    if errors:
        logger.warning(f"Transformation completed with {len(errors)} errors")
        for error in errors:
            logger.warning(f"  - {error}")

    return df


if __name__ == '__main__':
    # Test transformation
    logging.basicConfig(level=logging.INFO)

    # Sample data
    sample_data = [
        {
            'id': 1,
            'title': 'Test Anime',
            'description': '<p>Test description with <b>HTML</b></p>',
            'episodes': 12,
            'averageScore': 85,
            'popularity': 50000,
            'genres': 'Action|Adventure|Sci-Fi',
            'format': 'TV',
            'siteUrl': 'https://anilist.co/anime/1',
            'coverImage': 'https://example.com/image.jpg',
            'updatedAt': 1234567890
        }
    ]

    transformer = AnimeTransformer()
    df, errors = transformer.transform(sample_data)

    print(f"\nTransformed data:")
    print(df[['id', 'title', 'averageScore', 'score_percentile']].head())

    warehouse_data = transformer.prepare_for_warehouse(df)
    print(f"\nWarehouse tables prepared: {list(warehouse_data.keys())}")

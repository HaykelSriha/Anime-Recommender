"""
AniList API Extractor
=====================
Extracts anime data from the AniList GraphQL API with rate limiting,
pagination, and error handling.

API Documentation: https://anilist.gitbook.io/anilist-apiv2-docs/
"""

import time
import requests
from typing import List, Dict, Optional, Any
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)


class AniListExtractor:
    """
    Extracts anime data from AniList GraphQL API

    Features:
    - GraphQL query execution
    - Rate limiting (90 requests/minute)
    - Automatic pagination
    - Retry logic with exponential backoff
    - Incremental extraction support
    """

    def __init__(
        self,
        api_url: str = 'https://graphql.anilist.co',
        rate_limit: int = 90,
        page_size: int = 50,
        timeout: int = 30,
        max_retries: int = 3
    ):
        """
        Initialize AniList extractor

        Args:
            api_url: AniList GraphQL API endpoint
            rate_limit: Maximum requests per minute
            page_size: Number of results per page
            timeout: Request timeout in seconds
            max_retries: Maximum retry attempts on failure
        """
        self.api_url = api_url
        self.rate_limit = rate_limit
        self.page_size = page_size
        self.timeout = timeout
        self.max_retries = max_retries

        # Rate limiting state
        self.request_times: List[float] = []
        self.min_request_interval = 60.0 / rate_limit  # seconds between requests

        logger.info(f"Initialized AniList extractor (rate limit: {rate_limit}/min, page size: {page_size})")

    def _wait_for_rate_limit(self):
        """
        Enforce rate limiting by waiting if necessary
        """
        now = time.time()

        # Remove requests older than 1 minute
        self.request_times = [t for t in self.request_times if now - t < 60]

        # If at rate limit, wait
        if len(self.request_times) >= self.rate_limit:
            oldest_request = min(self.request_times)
            wait_time = 60 - (now - oldest_request)
            if wait_time > 0:
                logger.debug(f"Rate limit reached, waiting {wait_time:.2f}s")
                time.sleep(wait_time)

        # Add small delay between requests
        if self.request_times:
            time_since_last = now - max(self.request_times)
            if time_since_last < self.min_request_interval:
                time.sleep(self.min_request_interval - time_since_last)

        self.request_times.append(time.time())

    def _execute_query(self, query: str, variables: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute a GraphQL query with retry logic

        Args:
            query: GraphQL query string
            variables: Query variables

        Returns:
            Response data dictionary

        Raises:
            Exception: If all retry attempts fail
        """
        for attempt in range(self.max_retries):
            try:
                # Rate limiting
                self._wait_for_rate_limit()

                # Make request
                response = requests.post(
                    self.api_url,
                    json={'query': query, 'variables': variables},
                    timeout=self.timeout,
                    headers={'Content-Type': 'application/json'}
                )

                # Check for HTTP errors
                response.raise_for_status()

                # Parse response
                data = response.json()

                # Check for GraphQL errors
                if 'errors' in data:
                    error_msg = data['errors'][0]['message']
                    logger.error(f"GraphQL error: {error_msg}")
                    raise Exception(f"GraphQL error: {error_msg}")

                return data

            except requests.exceptions.Timeout:
                logger.warning(f"Request timeout (attempt {attempt + 1}/{self.max_retries})")
                if attempt < self.max_retries - 1:
                    time.sleep(2 ** attempt)  # Exponential backoff
                else:
                    raise

            except requests.exceptions.RequestException as e:
                logger.error(f"Request failed: {str(e)} (attempt {attempt + 1}/{self.max_retries})")
                if attempt < self.max_retries - 1:
                    time.sleep(2 ** attempt)
                else:
                    raise

    def extract_top_anime(self, limit: int = 1000, sort: str = 'POPULARITY_DESC') -> List[Dict[str, Any]]:
        """
        Extract top anime sorted by popularity, score, or other criteria

        Args:
            limit: Maximum number of anime to extract
            sort: Sort criteria (POPULARITY_DESC, SCORE_DESC, etc.)

        Returns:
            List of anime dictionaries
        """
        logger.info(f"Extracting top {limit} anime (sort: {sort})")

        query = """
        query ($page: Int, $perPage: Int, $sort: [MediaSort]) {
            Page(page: $page, perPage: $perPage) {
                pageInfo {
                    total
                    currentPage
                    lastPage
                    hasNextPage
                }
                media(type: ANIME, sort: $sort) {
                    id

                    # Basic info
                    title {
                        romaji
                        english
                        native
                    }
                    description(asHtml: false)

                    # Metrics
                    episodes
                    duration
                    averageScore
                    popularity
                    favourites

                    # Classification
                    genres
                    tags {
                        name
                        rank
                        category
                        isMediaSpoiler
                    }

                    # Production context
                    format
                    source
                    status
                    season
                    seasonYear

                    # Studios
                    studios {
                        nodes {
                            id
                            name
                            isAnimationStudio
                        }
                    }

                    # Staff (key creators)
                    staff(perPage: 5, sort: RELEVANCE) {
                        edges {
                            role
                            node {
                                id
                                name {
                                    full
                                }
                            }
                        }
                    }

                    # Characters (main cast)
                    characters(perPage: 10, sort: RELEVANCE) {
                        edges {
                            role
                            node {
                                id
                                name {
                                    full
                                }
                                gender
                            }
                        }
                    }

                    # Relations (sequels, prequels, etc.)
                    relations {
                        edges {
                            relationType
                            node {
                                id
                                title {
                                    romaji
                                    english
                                }
                            }
                        }
                    }

                    # Content rating
                    isAdult

                    # Other
                    siteUrl
                    coverImage {
                        large
                        medium
                    }
                    updatedAt
                }
            }
        }
        """

        all_anime = []
        page = 1
        has_next_page = True

        while has_next_page and len(all_anime) < limit:
            variables = {
                'page': page,
                'perPage': self.page_size,
                'sort': sort
            }

            try:
                data = self._execute_query(query, variables)

                page_info = data['data']['Page']['pageInfo']
                media_list = data['data']['Page']['media']

                # Process and add anime
                for anime in media_list:
                    if len(all_anime) >= limit:
                        break

                    # Flatten title (prefer English, fallback to Romaji)
                    title = anime['title']['english'] or anime['title']['romaji']

                    # Flatten cover image
                    cover_image = anime['coverImage']['large']

                    # Extract relations as JSON string
                    relations = None
                    if anime.get('relations') and anime['relations'].get('edges'):
                        relations = str([
                            {
                                'relationType': rel['relationType'],
                                'animeId': rel['node']['id'],
                                'animeTitle': rel['node']['title']['english'] or rel['node']['title']['romaji']
                            }
                            for rel in anime['relations']['edges']
                        ])

                    processed_anime = {
                        'id': anime['id'],
                        'title': title,
                        'description': anime['description'],
                        'episodes': anime['episodes'],
                        'averageScore': anime['averageScore'],
                        'popularity': anime['popularity'],
                        'genres': '|'.join(anime['genres']) if anime['genres'] else None,
                        'format': anime['format'],
                        'siteUrl': anime['siteUrl'],
                        'coverImage': cover_image,
                        'relations': relations,
                        'updatedAt': anime['updatedAt']
                    }

                    all_anime.append(processed_anime)

                logger.info(f"Extracted page {page}/{page_info['lastPage']}: {len(media_list)} anime (total: {len(all_anime)})")

                # Check if there's another page
                has_next_page = page_info['hasNextPage'] and len(all_anime) < limit
                page += 1

            except Exception as e:
                logger.error(f"Failed to extract page {page}: {str(e)}")
                raise

        logger.info(f"Successfully extracted {len(all_anime)} anime")
        return all_anime

    def extract_incremental(self, since_timestamp: int) -> List[Dict[str, Any]]:
        """
        Extract anime updated since a specific timestamp (incremental load)

        Args:
            since_timestamp: Unix timestamp (seconds) to fetch updates since

        Returns:
            List of updated anime dictionaries
        """
        logger.info(f"Extracting anime updated since {datetime.fromtimestamp(since_timestamp)}")

        query = """
        query ($page: Int, $perPage: Int, $updatedSince: Int) {
            Page(page: $page, perPage: $perPage) {
                pageInfo {
                    total
                    currentPage
                    lastPage
                    hasNextPage
                }
                media(type: ANIME, sort: UPDATED_AT_DESC) {
                    id
                    title {
                        romaji
                        english
                        native
                    }
                    description
                    episodes
                    averageScore
                    popularity
                    genres
                    format
                    siteUrl
                    coverImage {
                        large
                    }
                    relations {
                        edges {
                            relationType
                            node {
                                id
                                title {
                                    romaji
                                    english
                                }
                            }
                        }
                    }
                    updatedAt
                }
            }
        }
        """

        all_anime = []
        page = 1
        has_next_page = True

        while has_next_page:
            variables = {
                'page': page,
                'perPage': self.page_size,
                'updatedSince': since_timestamp
            }

            try:
                data = self._execute_query(query, variables)

                page_info = data['data']['Page']['pageInfo']
                media_list = data['data']['Page']['media']

                # Filter by updatedAt (API doesn't support this directly)
                for anime in media_list:
                    if anime['updatedAt'] < since_timestamp:
                        has_next_page = False
                        break

                    # Process anime (same as extract_top_anime)
                    title = anime['title']['english'] or anime['title']['romaji']
                    cover_image = anime['coverImage']['large']

                    # Extract relations as JSON string
                    relations = None
                    if anime.get('relations') and anime['relations'].get('edges'):
                        relations = str([
                            {
                                'relationType': rel['relationType'],
                                'animeId': rel['node']['id'],
                                'animeTitle': rel['node']['title']['english'] or rel['node']['title']['romaji']
                            }
                            for rel in anime['relations']['edges']
                        ])

                    processed_anime = {
                        'id': anime['id'],
                        'title': title,
                        'description': anime['description'],
                        'episodes': anime['episodes'],
                        'averageScore': anime['averageScore'],
                        'popularity': anime['popularity'],
                        'genres': '|'.join(anime['genres']) if anime['genres'] else None,
                        'format': anime['format'],
                        'siteUrl': anime['siteUrl'],
                        'coverImage': cover_image,
                        'relations': relations,
                        'updatedAt': anime['updatedAt']
                    }

                    all_anime.append(processed_anime)

                logger.info(f"Extracted page {page}: {len(media_list)} anime (total: {len(all_anime)})")

                # Check if there's another page
                has_next_page = has_next_page and page_info['hasNextPage']
                page += 1

            except Exception as e:
                logger.error(f"Failed to extract page {page}: {str(e)}")
                raise

        logger.info(f"Successfully extracted {len(all_anime)} updated anime")
        return all_anime

    def extract_by_ids(self, anime_ids: List[int]) -> List[Dict[str, Any]]:
        """
        Extract specific anime by their IDs

        Args:
            anime_ids: List of AniList anime IDs

        Returns:
            List of anime dictionaries
        """
        logger.info(f"Extracting {len(anime_ids)} anime by ID")

        query = """
        query ($ids: [Int]) {
            Page(page: 1, perPage: 50) {
                media(type: ANIME, id_in: $ids) {
                    id
                    title {
                        romaji
                        english
                    }
                    description
                    episodes
                    averageScore
                    popularity
                    genres
                    format
                    siteUrl
                    coverImage {
                        large
                    }
                    relations {
                        edges {
                            relationType
                            node {
                                id
                                title {
                                    romaji
                                    english
                                }
                            }
                        }
                    }
                    updatedAt
                }
            }
        }
        """

        all_anime = []

        # Process in batches of 50 (API limit)
        for i in range(0, len(anime_ids), 50):
            batch = anime_ids[i:i+50]

            variables = {'ids': batch}

            try:
                data = self._execute_query(query, variables)
                media_list = data['data']['Page']['media']

                for anime in media_list:
                    title = anime['title']['english'] or anime['title']['romaji']
                    cover_image = anime['coverImage']['large']

                    # Extract relations as JSON string
                    relations = None
                    if anime.get('relations') and anime['relations'].get('edges'):
                        relations = str([
                            {
                                'relationType': rel['relationType'],
                                'animeId': rel['node']['id'],
                                'animeTitle': rel['node']['title']['english'] or rel['node']['title']['romaji']
                            }
                            for rel in anime['relations']['edges']
                        ])

                    processed_anime = {
                        'id': anime['id'],
                        'title': title,
                        'description': anime['description'],
                        'episodes': anime['episodes'],
                        'averageScore': anime['averageScore'],
                        'popularity': anime['popularity'],
                        'genres': '|'.join(anime['genres']) if anime['genres'] else None,
                        'format': anime['format'],
                        'siteUrl': anime['siteUrl'],
                        'coverImage': cover_image,
                        'relations': relations,
                        'updatedAt': anime['updatedAt']
                    }

                    all_anime.append(processed_anime)

                logger.info(f"Extracted batch {i//50 + 1}: {len(media_list)} anime")

            except Exception as e:
                logger.error(f"Failed to extract batch starting at ID {batch[0]}: {str(e)}")
                raise

        logger.info(f"Successfully extracted {len(all_anime)} anime by ID")
        return all_anime


# Convenience function for standalone use
def extract_anime_data(limit: int = 100, sort: str = 'POPULARITY_DESC') -> List[Dict[str, Any]]:
    """
    Convenience function to extract anime data

    Args:
        limit: Number of anime to extract
        sort: Sort criteria

    Returns:
        List of anime dictionaries
    """
    extractor = AniListExtractor()
    return extractor.extract_top_anime(limit=limit, sort=sort)


if __name__ == '__main__':
    # Test extraction
    logging.basicConfig(level=logging.INFO)

    extractor = AniListExtractor()
    anime_list = extractor.extract_top_anime(limit=10)

    print(f"\nExtracted {len(anime_list)} anime:")
    for anime in anime_list[:5]:
        print(f"  - {anime['title']}: {anime['averageScore']}/100, {anime['popularity']} popularity")

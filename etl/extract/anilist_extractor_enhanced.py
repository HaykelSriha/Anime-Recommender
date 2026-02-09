"""
Enhanced AniList Extractor
===========================
Fetches rich metadata for better similarity computation:
- Tags (more granular than genres)
- Studios (production company)
- Source material (manga, light novel, etc.)
- Staff (director, writer)
- Characters (character types)
- Themes and demographics
- Season/year context
"""

import time
import logging
from typing import List, Dict, Any
import requests

logger = logging.getLogger(__name__)


class EnhancedAniListExtractor:
    """Enhanced extractor with rich metadata for better recommendations"""

    def __init__(
        self,
        api_url: str = 'https://graphql.anilist.co',
        rate_limit: int = 90,
        page_size: int = 50,
        timeout: int = 30,
        max_retries: int = 3
    ):
        self.api_url = api_url
        self.rate_limit = rate_limit
        self.page_size = page_size
        self.timeout = timeout
        self.max_retries = max_retries

        self.request_times: List[float] = []
        self.min_request_interval = 60.0 / rate_limit

        logger.info(f"Initialized Enhanced AniList extractor")

    def _wait_for_rate_limit(self):
        """Enforce rate limiting"""
        now = time.time()
        self.request_times = [t for t in self.request_times if now - t < 60]

        if len(self.request_times) >= self.rate_limit:
            oldest_request = min(self.request_times)
            wait_time = 60 - (now - oldest_request)
            if wait_time > 0:
                logger.debug(f"Rate limit reached, waiting {wait_time:.2f}s")
                time.sleep(wait_time)

        if self.request_times:
            last_request = max(self.request_times)
            time_since_last = now - last_request
            if time_since_last < self.min_request_interval:
                time.sleep(self.min_request_interval - time_since_last)

        self.request_times.append(time.time())

    def _execute_query(self, query: str, variables: Dict[str, Any]) -> Dict[str, Any]:
        """Execute GraphQL query with retries"""
        for attempt in range(self.max_retries):
            try:
                self._wait_for_rate_limit()

                response = requests.post(
                    self.api_url,
                    json={'query': query, 'variables': variables},
                    timeout=self.timeout,
                    headers={'Content-Type': 'application/json'}
                )

                response.raise_for_status()
                data = response.json()

                if 'errors' in data:
                    error_msg = data['errors'][0]['message']
                    logger.error(f"GraphQL error: {error_msg}")
                    raise Exception(f"GraphQL error: {error_msg}")

                return data

            except requests.exceptions.Timeout:
                logger.warning(f"Request timeout (attempt {attempt + 1}/{self.max_retries})")
                if attempt < self.max_retries - 1:
                    time.sleep(2 ** attempt)
                else:
                    raise

            except requests.exceptions.RequestException as e:
                logger.error(f"Request failed: {str(e)} (attempt {attempt + 1}/{self.max_retries})")
                if attempt < self.max_retries - 1:
                    time.sleep(2 ** attempt)
                else:
                    raise

    def extract_top_anime_enhanced(self, limit: int = 1000, sort: str = 'POPULARITY_DESC') -> List[Dict[str, Any]]:
        """
        Extract anime with RICH metadata for better similarity

        New fields for similarity computation:
        - tags: More granular than genres (e.g., "Time Travel", "Psychological", "School Life")
        - studios: Production company (similar studios often have similar styles)
        - source: Original, Manga, Light Novel, Visual Novel, etc.
        - staff: Director, writer (auteur theory - similar creators → similar shows)
        - season/year: Temporal context
        - duration: Episode length (movie vs TV vs short)
        - isAdult: Content rating filter
        """
        logger.info(f"Extracting top {limit} anime with ENHANCED metadata")

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
                    duration          # NEW: Episode length (minutes)
                    averageScore
                    popularity
                    favourites        # NEW: Number of users who favorited

                    # Classification
                    genres            # Broad: Action, Drama, etc.
                    tags {            # NEW: Granular tags!
                        name          # e.g., "Time Travel", "Psychological"
                        rank          # Relevance score (0-100)
                        category      # "theme", "setting", "cast", etc.
                        isMediaSpoiler
                    }

                    # Production context
                    format            # TV, Movie, OVA, ONA, Special
                    source            # NEW: Manga, Light Novel, Original, etc.
                    status            # FINISHED, RELEASING, NOT_YET_RELEASED
                    season            # NEW: WINTER, SPRING, SUMMER, FALL
                    seasonYear        # NEW: Release year

                    # Studios (production company)
                    studios {         # NEW: Animation studios!
                        nodes {
                            id
                            name
                            isAnimationStudio
                        }
                    }

                    # Staff (creators)
                    staff(perPage: 5, sort: RELEVANCE) {  # NEW: Key staff!
                        edges {
                            role      # Director, Original Creator, Script, etc.
                            node {
                                id
                                name { full }
                            }
                        }
                    }

                    # Characters (for character-based similarity)
                    characters(perPage: 10, sort: RELEVANCE) {  # NEW: Main characters!
                        edges {
                            role      # MAIN, SUPPORTING, BACKGROUND
                            node {
                                id
                                name { full }
                                age
                                gender
                            }
                        }
                    }

                    # Content rating
                    isAdult           # NEW: Adult content flag

                    # Relations (sequels, prequels, adaptations)
                    relations {       # NEW: Related anime!
                        edges {
                            relationType  # SEQUEL, PREQUEL, ADAPTATION, etc.
                            node {
                                id
                                title { romaji }
                            }
                        }
                    }

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

                # Add to results
                all_anime.extend(media_list)

                logger.info(f"Fetched page {page}: {len(media_list)} anime (total: {len(all_anime)})")

                # Check if we should continue
                has_next_page = page_info['hasNextPage'] and len(all_anime) < limit
                page += 1

            except Exception as e:
                logger.error(f"Failed to fetch page {page}: {str(e)}")
                break

        # Trim to exact limit
        all_anime = all_anime[:limit]

        logger.info(f"Extraction complete: {len(all_anime)} anime with enhanced metadata")
        return all_anime

    def extract_incremental(self, since_timestamp: int) -> List[Dict[str, Any]]:
        """Extract anime updated since timestamp (for incremental updates)"""
        logger.info(f"Extracting anime updated since timestamp {since_timestamp}")

        # Same query as extract_top_anime_enhanced but filtered by updatedAt
        # Implementation similar to above...
        pass


if __name__ == '__main__':
    # Test enhanced extractor
    logging.basicConfig(level=logging.INFO)

    extractor = EnhancedAniListExtractor()
    anime_list = extractor.extract_top_anime_enhanced(limit=5)

    # Print sample
    print("\n" + "=" * 80)
    print("SAMPLE ENHANCED DATA")
    print("=" * 80)

    if anime_list:
        sample = anime_list[0]
        print(f"\nTitle: {sample['title']['romaji']}")
        print(f"Genres: {', '.join(sample.get('genres', []))}")
        print(f"\nTags ({len(sample.get('tags', []))}):")
        for tag in sample.get('tags', [])[:10]:
            print(f"  - {tag['name']} (rank: {tag['rank']})")

        print(f"\nStudios:")
        for studio in sample.get('studios', {}).get('nodes', []):
            print(f"  - {studio['name']}")

        print(f"\nStaff:")
        for edge in sample.get('staff', {}).get('edges', [])[:5]:
            print(f"  - {edge['role']}: {edge['node']['name']['full']}")

        print(f"\nCharacters:")
        for edge in sample.get('characters', {}).get('edges', [])[:5]:
            char = edge['node']
            print(f"  - {char['name']['full']} ({edge['role']})")

        print(f"\nSource: {sample.get('source', 'N/A')}")
        print(f"Season: {sample.get('season', 'N/A')} {sample.get('seasonYear', '')}")
        print(f"Duration: {sample.get('duration', 'N/A')} minutes")

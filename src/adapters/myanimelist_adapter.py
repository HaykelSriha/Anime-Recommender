"""
MyAnimeList GraphQL API adapter for anime extraction.

Fetches anime data from MAL GraphQL API.
Note: MAL requires careful handling due to CORS and rate limiting.
Rate limit: 60 requests/minute (unofficial, be conservative)
"""

from typing import List, Dict, Any, Optional
import requests
import logging
import time

from .base_adapter import BaseAdapter

logger = logging.getLogger(__name__)


class MyAnimeListAdapter(BaseAdapter):
    """MyAnimeList GraphQL API adapter."""

    def __init__(self):
        """Initialize MyAnimeList adapter."""
        super().__init__(name="myanimelist", requests_per_minute=60)
        # MAL GraphQL endpoint (unofficial but works for public data)
        self.api_url = "https://api.myanimelist.net/v2"
        self.session = requests.Session()
        # Add headers to mimic browser
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        })

    def extract(
        self, limit: Optional[int] = None, incremental: bool = False
    ) -> List[Dict[str, Any]]:
        """
        Extract anime from MyAnimeList.

        Args:
            limit: Max anime to extract
            incremental: Only fetch updated anime (currently not implemented for MAL)

        Returns:
            List of standardized anime dicts
        """
        all_anime = []
        offset = 0
        per_page = 100

        logger.info(f"Starting MyAnimeList extraction (limit={limit})")

        while True:
            try:
                self.rate_limiter.wait_if_needed()

                # REST endpoint to get anime by rank
                url = f"{self.api_url}/anime/ranking"
                params = {
                    "ranking_type": "all",
                    "offset": offset,
                    "limit": per_page,
                    "fields": (
                        "id,title,main_picture,alternative_titles,start_date,"
                        "synopsis,mean,rank,popularity,num_episodes,media_type,"
                        "status,genres,related_anime"
                    ),
                }

                response = self.session.get(url, params=params, timeout=30)
                response.raise_for_status()

                data = response.json()
                anime_list = data.get("data", [])

                if not anime_list:
                    logger.warning(f"No anime returned at offset {offset}")
                    break

                # Parse anime
                for item in anime_list:
                    anime_obj = item.get("node", {})
                    anime = self._parse_anime(anime_obj)
                    if self.validate_anime(anime):
                        all_anime.append(anime)
                        self.extracted_count += 1
                    else:
                        self.error_count += 1

                logger.info(
                    f"Extracted offset {offset} "
                    f"({self.extracted_count} total)"
                )

                # Check limits
                if limit and self.extracted_count >= limit:
                    logger.info(f"Reached extraction limit: {limit}")
                    break

                # Check if more pages
                paging = data.get("paging", {})
                if "next" not in paging:
                    logger.info("No more pages available")
                    break

                offset += per_page

            except requests.RequestException as e:
                logger.error(f"Request error at offset {offset}: {e}")
                self.error_count += 1
                # Exponential backoff for rate limiting
                wait_time = min(2 ** 3, 60)  # Max 60s wait
                logger.info(f"Rate limited, waiting {wait_time}s...")
                time.sleep(wait_time)
            except Exception as e:
                logger.error(f"Error processing offset {offset}: {e}")
                self.error_count += 1
                break

        self.log_extraction_stats()
        return all_anime

    def _parse_anime(self, anime_obj: Dict[str, Any]) -> Dict[str, Any]:
        """
        Parse MAL anime object into standardized format.

        Args:
            anime_obj: MAL anime GraphQL/REST response

        Returns:
            Standardized anime dict
        """
        # Extract genres
        genres = []
        for genre_obj in anime_obj.get("genres", []):
            if isinstance(genre_obj, dict):
                genres.append(genre_obj.get("name", ""))
            else:
                genres.append(str(genre_obj))

        # Extract year from start_date
        year = None
        start_date = anime_obj.get("start_date")
        if start_date and len(start_date) >= 4:
            try:
                year = int(start_date[:4])
            except (ValueError, IndexError):
                year = None

        # Handle alternative titles
        alt_titles = anime_obj.get("alternative_titles", {})
        english_title = alt_titles.get("en") if isinstance(alt_titles, dict) else None
        japanese_title = alt_titles.get("ja") if isinstance(alt_titles, dict) else None

        return {
            "source": "myanimelist",
            "source_id": anime_obj.get("id"),
            "title": anime_obj.get("title", "Unknown"),
            "english_title": english_title,
            "japanese_title": japanese_title,
            "description": anime_obj.get("synopsis", "").replace("<br>", "\n"),
            "genres": genres,
            "year": year,
            "format": anime_obj.get("media_type", "UNKNOWN"),
            "episodes": anime_obj.get("num_episodes"),
            "average_score": self.standardize_score(
                anime_obj.get("mean", 0), max_score=10.0  # MAL uses 0-10 scale
            ),
            "popularity": anime_obj.get("popularity", 0),
            "image_url": anime_obj.get("main_picture", {}).get("large"),
            "relations": self._parse_relations(anime_obj.get("related_anime", [])),
        }

    def _parse_relations(self, related: List[Dict]) -> List[Dict[str, Any]]:
        """
        Parse MAL related anime into standardized format.

        Args:
            related: List of related anime from MAL

        Returns:
            List of relation dicts
        """
        relations = []
        for rel in related:
            relation = {
                "type": rel.get("relation_type", "UNKNOWN"),
                "id": rel.get("node", {}).get("id"),
                "title": rel.get("node", {}).get("title", ""),
            }
            relations.append(relation)
        return relations


# Quick test
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    adapter = MyAnimeListAdapter()
    # Note: This may fail due to CORS/rate limiting without proper setup
    try:
        anime = adapter.extract(limit=5)
        print(f"\nExtracted {len(anime)} anime:")
        for a in anime[:3]:
            print(f"  - {a['title']} ({a['source_id']})")
    except Exception as e:
        logger.error(f"Extraction failed: {e}")
        logger.info("MyAnimeList API may require additional setup or be rate-limited")

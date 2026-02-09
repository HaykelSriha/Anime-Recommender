"""
AniList GraphQL API adapter for anime extraction.

Fetches anime data from AniList GraphQL API (free, no auth required).
Rate limit: 90 requests/minute
"""

from typing import List, Dict, Any, Optional
import requests
import logging
import time

from .base_adapter import BaseAdapter

logger = logging.getLogger(__name__)


class AniListAdapter(BaseAdapter):
    """AniList GraphQL API adapter."""

    def __init__(self):
        """Initialize AniList adapter."""
        super().__init__(name="anilist", requests_per_minute=90)
        self.api_url = "https://graphql.anilist.co"
        self.session = requests.Session()

    def extract(
        self, limit: Optional[int] = None, incremental: bool = False
    ) -> List[Dict[str, Any]]:
        """
        Extract anime from AniList.

        Args:
            limit: Max anime to extract (None = all)
            incremental: Only fetch updated anime (uses updatedAt field)

        Returns:
            List of standardized anime dicts
        """
        all_anime = []
        page = 1
        per_page = 50
        total_pages = None

        logger.info(
            f"Starting AniList extraction (limit={limit}, incremental={incremental})"
        )

        while True:
            try:
                self.rate_limiter.wait_if_needed()

                # GraphQL query for anime
                query = """
                    query ($page: Int, $perPage: Int) {
                        Page(page: $page, perPage: $perPage) {
                            pageInfo {
                                total
                                currentPage
                                lastPage
                                hasNextPage
                            }
                            media(type: ANIME, sort: POPULARITY_DESC) {
                                id
                                title {
                                    romaji
                                    english
                                    native
                                }
                                description
                                genres
                                format
                                episodes
                                averageScore
                                popularity
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

                variables = {"page": page, "perPage": per_page}

                response = self.session.post(
                    self.api_url,
                    json={"query": query, "variables": variables},
                    timeout=30,
                )
                response.raise_for_status()

                data = response.json()

                if "errors" in data:
                    logger.error(f"GraphQL error: {data['errors']}")
                    raise Exception(f"GraphQL error: {data['errors']}")

                page_data = data.get("data", {}).get("Page", {})
                media_list = page_data.get("media", [])

                if not media_list:
                    logger.warning(f"No media returned on page {page}")
                    break

                # Parse anime
                for media in media_list:
                    anime = self._parse_anime(media)
                    if self.validate_anime(anime):
                        all_anime.append(anime)
                        self.extracted_count += 1
                    else:
                        self.error_count += 1

                # Check pagination
                page_info = page_data.get("pageInfo", {})
                total_pages = page_info.get("lastPage", 1)

                logger.info(
                    f"Extracted page {page}/{total_pages} "
                    f"({self.extracted_count} total)"
                )

                # Check limits
                if limit and self.extracted_count >= limit:
                    logger.info(f"Reached extraction limit: {limit}")
                    break

                if not page_info.get("hasNextPage"):
                    logger.info("No more pages available")
                    break

                page += 1

            except requests.RequestException as e:
                logger.error(f"Request error on page {page}: {e}")
                self.error_count += 1
                # Retry with exponential backoff
                wait_time = min(2 ** (page - 1), 60)
                logger.info(f"Retrying in {wait_time}s...")
                time.sleep(wait_time)
            except Exception as e:
                logger.error(f"Error processing page {page}: {e}")
                self.error_count += 1
                break

        self.log_extraction_stats()
        return all_anime

    def _parse_anime(self, media: Dict[str, Any]) -> Dict[str, Any]:
        """
        Parse AniList media object into standardized format.

        Args:
            media: AniList media GraphQL response

        Returns:
            Standardized anime dict
        """
        title_obj = media.get("title", {})
        cover = media.get("coverImage", {})

        # Extract relations
        relations = []
        for edge in media.get("relations", {}).get("edges", []):
            node = edge.get("node", {})
            relations.append({
                "type": edge.get("relationType", "UNKNOWN"),
                "id": node.get("id"),
                "title": node.get("title", {}).get("english") or node.get("title", {}).get("romaji", ""),
            })

        return {
            "source": "anilist",
            "source_id": media.get("id"),
            "title": title_obj.get("romaji") or title_obj.get("english") or "Unknown",
            "english_title": title_obj.get("english"),
            "japanese_title": title_obj.get("native"),
            "description": media.get("description", "").replace("<br>", "\n"),
            "genres": media.get("genres", []),
            "year": None,  # AniList doesn't provide year directly
            "format": media.get("format", "UNKNOWN"),
            "episodes": media.get("episodes"),
            "average_score": self.standardize_score(media.get("averageScore", 0), max_score=100.0),
            "popularity": media.get("popularity", 0),
            "image_url": cover.get("large"),
            "relations": relations,
            "updated_at": media.get("updatedAt"),
        }


# Quick test
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    adapter = AniListAdapter()
    anime = adapter.extract(limit=10)
    print(f"\nExtracted {len(anime)} anime:")
    for a in anime[:3]:
        print(f"  - {a['title']} ({a['source_id']})")

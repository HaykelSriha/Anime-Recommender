"""
Base adapter abstraction for multi-source anime extractors.

Defines the interface that all source adapters must implement:
- AniListAdapter
- MyAnimeListAdapter
- KitsuAdapter
- IMDBAdapter
"""

from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional
import time
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


class RateLimiter:
    """Simple rate limiter to respect API limits."""

    def __init__(self, requests_per_minute: int = 60):
        """
        Initialize rate limiter.

        Args:
            requests_per_minute: Max requests per minute (default 60)
        """
        self.requests_per_minute = requests_per_minute
        self.min_interval = 60.0 / requests_per_minute
        self.last_request_time = 0

    def wait_if_needed(self):
        """Wait if necessary to maintain rate limit."""
        elapsed = time.time() - self.last_request_time
        if elapsed < self.min_interval:
            wait_time = self.min_interval - elapsed
            logger.debug(f"Rate limiting: waiting {wait_time:.2f}s")
            time.sleep(wait_time)
        self.last_request_time = time.time()


class BaseAdapter(ABC):
    """
    Abstract base class for anime data extractors.

    All source-specific adapters must inherit from this and implement:
    - extract() method
    - API-specific authentication/pagination as needed
    """

    def __init__(self, name: str, requests_per_minute: int = 60):
        """
        Initialize adapter.

        Args:
            name: Source name (e.g., 'anilist', 'myanimelist', 'kitsu', 'imdb')
            requests_per_minute: Rate limit (API-dependent)
        """
        self.name = name
        self.rate_limiter = RateLimiter(requests_per_minute)
        self.extracted_count = 0
        self.error_count = 0

    @abstractmethod
    def extract(
        self, limit: Optional[int] = None, incremental: bool = False
    ) -> List[Dict[str, Any]]:
        """
        Extract anime data from source.

        Args:
            limit: Maximum number of anime to extract (None = all)
            incremental: If True, only fetch updated anime (since last run)

        Returns:
            List of anime dictionaries with standardized schema:
            {
                'source': str (e.g., 'anilist'),
                'source_id': int (original ID in source DB),
                'title': str,
                'english_title': str (optional),
                'japanese_title': str (optional),
                'description': str,
                'genres': list of str,
                'year': int,
                'format': str (TV, Movie, OVA, etc.),
                'episodes': int (or None for incomplete),
                'average_score': float (0-100 or 0-10),
                'popularity': int,
                'image_url': str (optional),
                'relations': list (optional) - [{'type': str, 'id': int, 'title': str}]
            }

        Raises:
            Exception: If extraction fails (connection, auth, etc.)
        """
        pass

    def validate_anime(self, anime: Dict[str, Any]) -> bool:
        """
        Validate anime data has required fields.

        Args:
            anime: Anime dictionary to validate

        Returns:
            True if valid, False otherwise
        """
        required_fields = {
            "source",
            "source_id",
            "title",
            "description",
            "genres",
            "average_score",
        }
        return all(field in anime for field in required_fields)

    def normalize_title(self, title: str) -> str:
        """
        Normalize anime title for deduplication.

        Removes common suffixes like season info.

        Args:
            title: Raw title from source

        Returns:
            Normalized title
        """
        import re

        # Remove season/part suffixes
        patterns = [
            r"\s+Season\s+\d+$",
            r"\s+S\d+$",
            r"\s+Part\s+\d+$",
            r"\s+\(Season\s+\d+\)$",
            r"\s+The\s+Final\s+Season$",
            r"\s+Final\s+Season$",
            r":\s*Season\s+\d+.*$",
            r":\s*Part\s+\d+.*$",
        ]

        normalized = title
        for pattern in patterns:
            normalized = re.sub(pattern, "", normalized, flags=re.IGNORECASE)

        return normalized.strip()

    def log_extraction_stats(self):
        """Log extraction statistics."""
        logger.info(
            f"{self.name}: extracted {self.extracted_count} anime, {self.error_count} errors"
        )

    @staticmethod
    def standardize_score(score: float, max_score: float = 100.0) -> float:
        """
        Normalize score to 0-100 scale.

        Args:
            score: Raw score from source
            max_score: Maximum score in source (e.g., 10 for some APIs)

        Returns:
            Score normalized to 0-100 scale
        """
        if max_score == 100.0:
            return score
        return (score / max_score) * 100.0

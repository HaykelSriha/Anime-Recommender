"""
Anime data extractors.

Adapters for ingesting anime data from AniList (GraphQL API).
"""

from .base_adapter import BaseAdapter, RateLimiter
from .deduplication import AnimeDeduplicator

__all__ = ["BaseAdapter", "RateLimiter", "AnimeDeduplicator"]

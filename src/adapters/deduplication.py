"""
Deduplication engine for cross-source anime matching.

Uses fuzzy title matching to build canonical anime mappings:
- AniList#16498 (Attack on Titan)
- MAL#16498 (Attack on Titan)
- Kitsu#7442 (Shingeki no Kyojin)
→ All map to canonical ID: AL_16498
"""

from typing import Dict, List, Tuple, Set
from fuzzywuzzy import fuzz
import logging

logger = logging.getLogger(__name__)


class AnimeDeduplicator:
    """Fuzzy matching engine for anime deduplication across sources."""

    def __init__(self, similarity_threshold: float = 0.85):
        """
        Initialize deduplicator.

        Args:
            similarity_threshold: Min similarity score (0-1) to match anime
                                 (0.85 = 85% match)
        """
        self.similarity_threshold = similarity_threshold
        self.dedup_map: Dict[str, str] = {}  # source_id → canonical_id
        self.canonical_anime: Dict[str, Dict] = {}  # canonical_id → anime_data
        self.match_history: List[Tuple[str, str, float]] = []

    def build_canonical_anime(self, all_anime: List[Dict]) -> Dict[str, Dict]:
        """
        Build canonical anime mapping from all sources.

        Algorithm:
        1. Sort anime by popularity (popular = more authoritative)
        2. First anime becomes canonical
        3. Match subsequent anime via fuzzy title matching
        4. Group similar anime together
        5. Keep best/most popular as canonical

        Args:
            all_anime: List of anime dicts from all sources
                      Must have: source, source_id, title, popularity

        Returns:
            Dict mapping canonical_id → canonical_anime_data
        """
        if not all_anime:
            logger.warning("No anime provided for deduplication")
            return {}

        # Sort by popularity (descending) to prefer well-known anime
        sorted_anime = sorted(all_anime, key=lambda x: x.get("popularity", 0), reverse=True)

        canonical_id_counter = 0
        grouped_anime: Dict[str, List[Dict]] = {}  # canonical_id → [anime list]

        for anime in sorted_anime:
            source = anime.get("source")
            source_id = anime.get("source_id")
            title = anime.get("title", "")

            if not source or not source_id or not title:
                logger.warning(f"Skipping anime with missing fields: {anime}")
                continue

            source_key = f"{source}#{source_id}"

            # Try to match with existing canonical groups
            matched_canonical_id = None
            best_similarity = 0

            for canonical_id, group in grouped_anime.items():
                # Compare with first anime in group (the canonical one)
                canonical_anime = group[0]
                similarity = self._fuzzy_match(title, canonical_anime["title"])

                if similarity > best_similarity:
                    best_similarity = similarity
                    matched_canonical_id = canonical_id

            # Decide: create new group or add to existing
            if (
                matched_canonical_id is not None
                and best_similarity >= self.similarity_threshold
            ):
                # Add to existing group
                grouped_anime[matched_canonical_id].append(anime)
                self.dedup_map[source_key] = matched_canonical_id
                self.match_history.append((source_key, matched_canonical_id, best_similarity))
                logger.debug(
                    f"Matched {source_key} to {matched_canonical_id} "
                    f"(similarity: {best_similarity:.2f})"
                )
            else:
                # Create new canonical group
                canonical_type = f"AL_{source_id}" if source == "anilist" else f"{source.upper()}_{canonical_id_counter}"
                grouped_anime[canonical_type] = [anime]
                self.dedup_map[source_key] = canonical_type
                logger.debug(f"Created canonical group: {canonical_type} for {source_key}")
                canonical_id_counter += 1

        # Build canonical anime dict (use first/best anime from each group)
        self.canonical_anime = {
            canonical_id: group[0] for canonical_id, group in grouped_anime.items()
        }

        logger.info(
            f"Deduplication complete: {len(all_anime)} anime → "
            f"{len(self.canonical_anime)} canonical anime"
        )

        return self.canonical_anime

    def _fuzzy_match(self, title1: str, title2: str) -> float:
        """
        Fuzzy match two anime titles.

        Uses token_sort_ratio to handle word reordering.

        Args:
            title1: First title
            title2: Second title

        Returns:
            Similarity score (0-1)
        """
        # Token sort handles "Shingeki no Kyojin" vs "Attack on Titan" type mismatches better
        # For same-language matches, also try substring matching
        token_sort_score = fuzz.token_sort_ratio(title1.lower(), title2.lower()) / 100.0

        # Partial matching for long titles
        partial_score = fuzz.partial_token_sort_ratio(
            title1.lower(), title2.lower()
        ) / 100.0

        # Return average of both methods
        return (token_sort_score + partial_score) / 2

    def get_canonical_id(self, source: str, source_id: int) -> str:
        """
        Get canonical ID for a source anime.

        Args:
            source: Source name (anilist, mal, kitsu, imdb)
            source_id: Original ID in source

        Returns:
            Canonical ID string
        """
        source_key = f"{source}#{source_id}"
        return self.dedup_map.get(source_key, source_key)

    def get_canonical_anime(self, canonical_id: str) -> Dict:
        """
        Get canonical anime data.

        Args:
            canonical_id: Canonical anime ID

        Returns:
            Anime dictionary with all attributes
        """
        return self.canonical_anime.get(canonical_id, {})

    def get_dedup_statistics(self) -> Dict[str, any]:
        """
        Get deduplication statistics.

        Returns:
            Dict with stats: total_anime, canonical_count, avg_matches_per_canonical
        """
        total_anime_sources = len(self.dedup_map)
        canonical_count = len(self.canonical_anime)

        avg_sources_per_canonical = (
            total_anime_sources / canonical_count if canonical_count > 0 else 0
        )

        return {
            "total_anime_sources": total_anime_sources,
            "canonical_anime": canonical_count,
            "avg_sources_per_canonical": avg_sources_per_canonical,
            "successful_matches": len([m for m in self.match_history if m[2] >= self.similarity_threshold]),
            "similarity_threshold": self.similarity_threshold,
        }

    def export_dedup_map(self) -> List[Dict]:
        """
        Export deduplication mapping for warehouse loading.

        Returns:
            List of dicts: [
                {
                    'source': str,
                    'source_id': int,
                    'canonical_id': str,
                    'confidence_score': float (0-1)
                }
            ]
        """
        result = []
        for source_key, (canonical_id, confidence) in zip(self.dedup_map.items(), [m[2] for m in self.match_history]):
            parts = source_key.split("#")
            if len(parts) == 2:
                result.append({
                    "source": parts[0],
                    "source_id": int(parts[1]),
                    "canonical_id": canonical_id,
                    "confidence_score": confidence,
                })
        return result

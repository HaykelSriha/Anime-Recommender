#!/usr/bin/env python
"""
Debug script to check why season deduplication isn't working
"""
import duckdb
import re
import sys

def get_base_series_name(title):
    """Extract base series name by removing season indicators"""
    patterns = [
        r'\s+Season\s+\d+',
        r'\s+S\d+',
        r'\s+Part\s+\d+',
        r'\s+\(Season\s+\d+\)',
        r'\s+The\s+Final\s+Season',
        r'\s+Final\s+Season',
    ]
    base = title
    for pattern in patterns:
        base = re.sub(pattern, '', base, flags=re.IGNORECASE)
    return base.strip()


print("=" * 80)
print("ANIME SEASON FILTER DEBUG")
print("=" * 80)

conn = duckdb.connect('warehouse/anime_dw.duckdb', read_only=True)

# Get sample anime titles to check regex patterns
print("\n1. Checking actual anime titles in database:")
print("-" * 80)
results = conn.execute('''
    SELECT anime_id, title
    FROM dim_anime
    ORDER BY title
    LIMIT 50
''').fetchall()

# Look for Attack on Titan variations
aot_titles = [title for _, title in results if 'attack' in title.lower() or 'titan' in title.lower()]
print(f"Found {len(aot_titles)} anime with 'Attack' or 'Titan' in title:")
for title in aot_titles:
    base = get_base_series_name(title)
    print(f"  '{title}' → BASE: '{base}'")

# Check all titles and see which ones have season indicators
print("\n2. Checking which anime titles contain season indicators:")
print("-" * 80)
season_titles = []
for _, title in results:
    base = get_base_series_name(title)
    if base != title:  # Title was modified (contains season indicator)
        season_titles.append((title, base))

print(f"Found {len(season_titles)} anime with season indicators:")
for title, base in season_titles[:20]:  # Show first 20
    print(f"  '{title}' → '{base}'")

if len(season_titles) > 20:
    print(f"  ... and {len(season_titles) - 20} more")

# Test the filtering logic
print("\n3. Testing filtering logic:")
print("-" * 80)

# Simulate user adding Attack on Titan Season 1 and Season 2
test_favorites = [
    "Attack on Titan",
    "Attack on Titan Season 2"
]

print(f"Test favorites: {test_favorites}")

# Build favorite base names
favorite_base_names = set()
for title in test_favorites:
    base_name = get_base_series_name(title)
    favorite_base_names.add(base_name.lower())
    print(f"  '{title}' → base: '{base_name}' (normalized: '{base_name.lower()}')")

print(f"\nFavorite base names set: {favorite_base_names}")

# Check if other seasons would be filtered
print(f"\nWould these anime be EXCLUDED from recommendations?")
for _, title in results[:30]:
    anime_base_name = get_base_series_name(title).lower()
    should_exclude = anime_base_name in favorite_base_names
    if should_exclude or 'attack' in title.lower():
        status = "✓ EXCLUDED" if should_exclude else "✗ INCLUDED"
        print(f"  {status}: '{title}' (base: '{anime_base_name}')")

conn.close()

print("\n" + "=" * 80)
print("DEBUG COMPLETE")
print("=" * 80)

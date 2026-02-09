-- Schema migration: Add multi-source anime tracking to dim_anime

ALTER TABLE dim_anime ADD COLUMN IF NOT EXISTS source_anime_id INTEGER;
ALTER TABLE dim_anime ADD COLUMN IF NOT EXISTS canonical_anime_id VARCHAR;
ALTER TABLE dim_anime ADD COLUMN IF NOT EXISTS data_sources VARCHAR;
ALTER TABLE dim_anime ADD COLUMN IF NOT EXISTS confidence_score FLOAT DEFAULT 1.0;

-- Create dimension table for data sources
CREATE TABLE IF NOT EXISTS dim_source (
    source_key VARCHAR PRIMARY KEY,
    source_name VARCHAR NOT NULL,
    api_url VARCHAR,
    rate_limit_per_minute INTEGER,
    last_extraction TIMESTAMP,
    description VARCHAR
);

-- Initialize source dimension
INSERT OR IGNORE INTO dim_source VALUES
    ('anilist', 'AniList', 'https://graphql.anilist.co', 90, NULL, 'AniList GraphQL API'),
    ('myanimelist', 'MyAnimeList', 'https://api.myanimelist.net/v2', 60, NULL, 'MyAnimeList REST API'),
    ('kitsu', 'Kitsu', 'https://kitsu.io/api/edge', 60, NULL, 'Kitsu REST API'),
    ('imdb', 'IMDB', NULL, 30, NULL, 'IMDB custom data source'),
    ('reddit', 'Reddit', NULL, NULL, NULL, 'Reddit discussions (future)');

-- Bridge table: Anime deduplication mapping
CREATE TABLE IF NOT EXISTS bridge_anime_deduplication (
    source VARCHAR NOT NULL,
    source_anime_id INTEGER NOT NULL,
    canonical_anime_id VARCHAR NOT NULL,
    confidence_score FLOAT DEFAULT 1.0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (source, source_anime_id)
);

-- Create indexes
CREATE INDEX IF NOT EXISTS idx_bridge_anime_dedup_canonical
    ON bridge_anime_deduplication(canonical_anime_id);
CREATE INDEX IF NOT EXISTS idx_bridge_anime_dedup_confidence
    ON bridge_anime_deduplication(confidence_score DESC);

-- Stats table
CREATE TABLE IF NOT EXISTS etl_deduplication_stats (
    source VARCHAR PRIMARY KEY,
    anime_extracted INTEGER,
    anime_deduplicated INTEGER,
    avg_confidence_score FLOAT,
    extraction_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

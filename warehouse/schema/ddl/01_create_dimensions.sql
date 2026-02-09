-- ==============================================================================
-- DIMENSION TABLES FOR ANIME DATA WAREHOUSE
-- ==============================================================================
-- This script creates dimension tables for the anime data warehouse using a
-- star schema design. Dimensions contain descriptive attributes that provide
-- context for facts.
-- ==============================================================================

-- ------------------------------------------------------------------------------
-- Anime Dimension (SCD Type 2)
-- ------------------------------------------------------------------------------
-- Contains core anime attributes with historical tracking
-- Uses Slowly Changing Dimension Type 2 to track changes over time
-- ------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS dim_anime (
    anime_key INTEGER PRIMARY KEY,              -- Surrogate key
    anime_id INTEGER NOT NULL,                  -- Natural key from AniList
    title VARCHAR NOT NULL,
    description TEXT,
    site_url VARCHAR,
    cover_image_url VARCHAR,

    -- ENHANCED METADATA for better similarity
    tags TEXT,                                   -- Pipe-separated tags (e.g., "Military|Gore|Tragedy")
    studios TEXT,                                -- Pipe-separated studio names
    staff TEXT,                                  -- Pipe-separated "role:name" pairs
    characters TEXT,                             -- Pipe-separated main character names
    source VARCHAR,                              -- MANGA, LIGHT_NOVEL, ORIGINAL, etc.
    season VARCHAR,                              -- WINTER, SPRING, SUMMER, FALL
    season_year INTEGER,                         -- Release year
    duration INTEGER,                            -- Episode duration in minutes
    favourites INTEGER,                          -- Number of users who favorited
    is_adult BOOLEAN,                            -- Adult content flag

    -- SCD Type 2 columns for historical tracking
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_current BOOLEAN DEFAULT TRUE,            -- TRUE for current version
    effective_date DATE DEFAULT CURRENT_DATE,   -- When this version became active
    expiration_date DATE DEFAULT '9999-12-31',  -- When this version expired

    -- Data quality and lineage
    data_source VARCHAR DEFAULT 'AniList',
    data_quality_score DECIMAL(3,2)             -- 0.00 to 1.00
);

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_anime_id ON dim_anime(anime_id);
CREATE INDEX IF NOT EXISTS idx_anime_current ON dim_anime(is_current);
CREATE INDEX IF NOT EXISTS idx_anime_effective_date ON dim_anime(effective_date);
CREATE INDEX IF NOT EXISTS idx_anime_title ON dim_anime(title);

-- ------------------------------------------------------------------------------
-- Genre Dimension
-- ------------------------------------------------------------------------------
-- Reference table for anime genres
-- ------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS dim_genre (
    genre_key INTEGER PRIMARY KEY,
    genre_name VARCHAR NOT NULL UNIQUE,
    genre_category VARCHAR,                     -- e.g., 'Theme', 'Demographic', 'Genre'
    genre_description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_genre_name ON dim_genre(genre_name);

-- ------------------------------------------------------------------------------
-- Format Dimension
-- ------------------------------------------------------------------------------
-- Reference table for anime formats (TV, Movie, OVA, etc.)
-- ------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS dim_format (
    format_key INTEGER PRIMARY KEY,
    format_name VARCHAR NOT NULL UNIQUE,
    format_description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_format_name ON dim_format(format_name);

-- ==============================================================================
-- SEED DATA FOR REFERENCE DIMENSIONS
-- ==============================================================================

-- Seed common formats
INSERT INTO dim_format (format_key, format_name, format_description) VALUES
    (1, 'TV', 'Television series'),
    (2, 'Movie', 'Theatrical film'),
    (3, 'OVA', 'Original Video Animation'),
    (4, 'ONA', 'Original Net Animation'),
    (5, 'Special', 'Special episode'),
    (6, 'Music', 'Music video')
ON CONFLICT (format_name) DO NOTHING;

-- Seed common genres
INSERT INTO dim_genre (genre_key, genre_name, genre_category) VALUES
    (1, 'Action', 'Genre'),
    (2, 'Adventure', 'Genre'),
    (3, 'Comedy', 'Genre'),
    (4, 'Drama', 'Genre'),
    (5, 'Fantasy', 'Genre'),
    (6, 'Horror', 'Genre'),
    (7, 'Mystery', 'Genre'),
    (8, 'Psychological', 'Genre'),
    (9, 'Romance', 'Genre'),
    (10, 'Sci-Fi', 'Genre'),
    (11, 'Slice of Life', 'Genre'),
    (12, 'Sports', 'Genre'),
    (13, 'Supernatural', 'Genre'),
    (14, 'Thriller', 'Genre'),
    (15, 'Mecha', 'Theme'),
    (16, 'Music', 'Theme'),
    (17, 'School', 'Theme'),
    (18, 'Military', 'Theme'),
    (19, 'Shounen', 'Demographic'),
    (20, 'Shoujo', 'Demographic'),
    (21, 'Seinen', 'Demographic'),
    (22, 'Josei', 'Demographic')
ON CONFLICT (genre_name) DO NOTHING;

-- ==============================================================================
-- DIMENSION TABLE VIEWS
-- ==============================================================================

-- View for current anime (most recent version)
CREATE OR REPLACE VIEW vw_dim_anime_current AS
SELECT
    anime_key,
    anime_id,
    title,
    description,
    site_url,
    cover_image_url,
    created_at,
    updated_at,
    effective_date,
    data_source,
    data_quality_score
FROM dim_anime
WHERE is_current = TRUE;

-- ==============================================================================
-- END OF SCRIPT
-- ==============================================================================

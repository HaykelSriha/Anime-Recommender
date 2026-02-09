-- ==============================================================================
-- BRIDGE TABLES FOR ANIME DATA WAREHOUSE
-- ==============================================================================
-- Bridge tables handle many-to-many relationships between dimensions.
-- They act as junction tables connecting two dimension tables.
-- ==============================================================================

-- ------------------------------------------------------------------------------
-- Bridge: Anime-Genre Relationship
-- ------------------------------------------------------------------------------
-- Handles the many-to-many relationship between anime and genres
-- One anime can have multiple genres, and one genre can apply to multiple anime
-- ------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS bridge_anime_genre (
    anime_key INTEGER NOT NULL,
    genre_key INTEGER NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (anime_key, genre_key),

    -- Foreign keys
    FOREIGN KEY (anime_key) REFERENCES dim_anime(anime_key),
    FOREIGN KEY (genre_key) REFERENCES dim_genre(genre_key)
);

-- Create indexes for bi-directional queries
CREATE INDEX IF NOT EXISTS idx_bridge_anime ON bridge_anime_genre(anime_key);
CREATE INDEX IF NOT EXISTS idx_bridge_genre ON bridge_anime_genre(genre_key);

-- ==============================================================================
-- BRIDGE TABLE VIEWS
-- ==============================================================================

-- View: Anime with their genres (aggregated)
CREATE OR REPLACE VIEW vw_anime_with_genres AS
SELECT
    a.anime_key,
    a.anime_id,
    a.title,
    STRING_AGG(g.genre_name, '|' ORDER BY g.genre_name) AS genres,
    COUNT(DISTINCT g.genre_key) AS genre_count
FROM dim_anime a
LEFT JOIN bridge_anime_genre bg ON a.anime_key = bg.anime_key
LEFT JOIN dim_genre g ON bg.genre_key = g.genre_key
WHERE a.is_current = TRUE
GROUP BY a.anime_key, a.anime_id, a.title;

-- View: Genres with anime count
CREATE OR REPLACE VIEW vw_genre_popularity AS
SELECT
    g.genre_key,
    g.genre_name,
    g.genre_category,
    COUNT(DISTINCT bg.anime_key) AS anime_count
FROM dim_genre g
LEFT JOIN bridge_anime_genre bg ON g.genre_key = bg.genre_key
LEFT JOIN dim_anime a ON bg.anime_key = a.anime_key AND a.is_current = TRUE
GROUP BY g.genre_key, g.genre_name, g.genre_category
ORDER BY anime_count DESC;

-- View: Anime by genre (for filtering)
CREATE OR REPLACE VIEW vw_anime_by_genre AS
SELECT
    g.genre_name,
    a.anime_key,
    a.anime_id,
    a.title,
    a.description,
    a.cover_image_url
FROM dim_genre g
JOIN bridge_anime_genre bg ON g.genre_key = bg.genre_key
JOIN dim_anime a ON bg.anime_key = a.anime_key
WHERE a.is_current = TRUE;

-- ==============================================================================
-- END OF SCRIPT
-- ==============================================================================

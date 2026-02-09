-- ==============================================================================
-- ANALYTICAL VIEWS FOR ANIME DATA WAREHOUSE
-- ==============================================================================
-- This script creates comprehensive views that join dimensions and facts
-- to provide easy access to commonly needed data for the application.
-- ==============================================================================

-- ------------------------------------------------------------------------------
-- View: Current Anime with All Dimensions
-- ------------------------------------------------------------------------------
-- Main view combining anime dimensions with latest metrics
-- This is the primary view used by the Streamlit application
-- ------------------------------------------------------------------------------

CREATE OR REPLACE VIEW vw_anime_current AS
SELECT
    -- Anime dimension
    a.anime_key,
    a.anime_id,
    a.title,
    a.description,
    a.site_url AS siteUrl,                      -- Match CSV column name
    a.cover_image_url AS coverImage,            -- Match CSV column name

    -- ENHANCED metadata for better similarity
    a.tags,
    a.studios,
    a.staff,
    a.characters,
    a.source,
    a.season,
    a.season_year,
    a.duration,
    a.favourites,
    a.is_adult,

    -- Format dimension
    fmt.format_name AS format,

    -- Latest metrics
    m.average_score AS averageScore,            -- Match CSV column name
    m.popularity,
    m.episodes,
    m.duration_minutes,
    m.favorites,
    m.score_percentile,
    m.popularity_rank,
    m.snapshot_date,

    -- Aggregated genres (pipe-delimited string to match CSV format)
    STRING_AGG(g.genre_name, '|' ORDER BY g.genre_name) AS genres,

    -- Metadata
    a.data_quality_score,
    a.effective_date,
    m.created_at AS metrics_last_updated

FROM dim_anime a

-- Join latest metrics
LEFT JOIN (
    SELECT DISTINCT ON (anime_key)
        anime_key,
        format_key,
        average_score,
        popularity,
        episodes,
        duration_minutes,
        favorites,
        score_percentile,
        popularity_rank,
        snapshot_date,
        created_at
    FROM fact_anime_metrics
    ORDER BY anime_key, snapshot_date DESC
) m ON a.anime_key = m.anime_key

-- Join format dimension
LEFT JOIN dim_format fmt ON m.format_key = fmt.format_key

-- Join genres through bridge table
LEFT JOIN bridge_anime_genre bg ON a.anime_key = bg.anime_key
LEFT JOIN dim_genre g ON bg.genre_key = g.genre_key

-- Only current anime versions
WHERE a.is_current = TRUE

GROUP BY
    a.anime_key, a.anime_id, a.title, a.description,
    a.site_url, a.cover_image_url,
    a.tags, a.studios, a.staff, a.characters,
    a.source, a.season, a.season_year, a.duration,
    a.favourites, a.is_adult,
    fmt.format_name,
    m.average_score, m.popularity, m.episodes,
    m.duration_minutes, m.favorites, m.score_percentile,
    m.popularity_rank, m.snapshot_date, a.data_quality_score,
    a.effective_date, m.created_at;

-- ------------------------------------------------------------------------------
-- View: Top Rated Anime
-- ------------------------------------------------------------------------------
-- Anime sorted by average score (descending)
-- ------------------------------------------------------------------------------

CREATE OR REPLACE VIEW vw_top_rated_anime AS
SELECT
    anime_key,
    anime_id,
    title,
    description,
    averageScore,
    popularity,
    episodes,
    format,
    genres,
    coverImage,
    siteUrl,
    ROW_NUMBER() OVER (ORDER BY averageScore DESC, popularity DESC) AS rank
FROM vw_anime_current
WHERE averageScore IS NOT NULL
ORDER BY averageScore DESC, popularity DESC;

-- ------------------------------------------------------------------------------
-- View: Most Popular Anime
-- ------------------------------------------------------------------------------
-- Anime sorted by popularity (descending)
-- ------------------------------------------------------------------------------

CREATE OR REPLACE VIEW vw_most_popular_anime AS
SELECT
    anime_key,
    anime_id,
    title,
    description,
    averageScore,
    popularity,
    episodes,
    format,
    genres,
    coverImage,
    siteUrl,
    ROW_NUMBER() OVER (ORDER BY popularity DESC) AS rank
FROM vw_anime_current
WHERE popularity IS NOT NULL
ORDER BY popularity DESC;

-- ------------------------------------------------------------------------------
-- View: Anime Recommendations (Pre-computed)
-- ------------------------------------------------------------------------------
-- Top 10 recommendations for each anime based on similarity scores
-- ------------------------------------------------------------------------------

CREATE OR REPLACE VIEW vw_anime_recommendations AS
WITH ranked_recommendations AS (
    SELECT
        s.anime_key_1,
        s.anime_key_2,
        s.similarity_score,
        ROW_NUMBER() OVER (
            PARTITION BY s.anime_key_1
            ORDER BY s.similarity_score DESC
        ) AS rec_rank
    FROM fact_anime_similarity s
)
SELECT
    r.anime_key_1,
    a1.anime_id AS source_anime_id,
    a1.title AS source_anime_title,
    r.anime_key_2,
    a2.anime_id AS recommended_anime_id,
    a2.title AS recommended_anime_title,
    a2.description AS recommended_anime_description,
    a2.averageScore AS recommended_anime_score,
    a2.popularity AS recommended_anime_popularity,
    a2.genres AS recommended_anime_genres,
    a2.coverImage AS recommended_anime_cover,
    a2.siteUrl AS recommended_anime_url,
    r.similarity_score,
    r.rec_rank
FROM ranked_recommendations r
JOIN vw_anime_current a1 ON r.anime_key_1 = a1.anime_key
JOIN vw_anime_current a2 ON r.anime_key_2 = a2.anime_key
WHERE r.rec_rank <= 10;

-- ------------------------------------------------------------------------------
-- View: Anime by Genre (Filtered)
-- ------------------------------------------------------------------------------
-- Helper view for filtering anime by genre
-- ------------------------------------------------------------------------------

CREATE OR REPLACE VIEW vw_anime_by_genre_expanded AS
SELECT DISTINCT
    g.genre_name,
    g.genre_category,
    a.anime_key,
    a.anime_id,
    a.title,
    a.description,
    a.averageScore,
    a.popularity,
    a.episodes,
    a.format,
    a.genres,
    a.coverImage,
    a.siteUrl
FROM dim_genre g
JOIN bridge_anime_genre bg ON g.genre_key = bg.genre_key
JOIN vw_anime_current a ON bg.anime_key = a.anime_key
ORDER BY g.genre_name, a.popularity DESC;

-- ------------------------------------------------------------------------------
-- View: Anime Statistics Summary
-- ------------------------------------------------------------------------------
-- Overall statistics about the anime catalog
-- ------------------------------------------------------------------------------

CREATE OR REPLACE VIEW vw_anime_statistics AS
SELECT
    COUNT(*) AS total_anime,
    COUNT(DISTINCT format) AS total_formats,
    AVG(averageScore) AS avg_score,
    MAX(averageScore) AS max_score,
    MIN(averageScore) AS min_score,
    AVG(popularity) AS avg_popularity,
    MAX(popularity) AS max_popularity,
    AVG(episodes) AS avg_episodes,
    MAX(episodes) AS max_episodes,
    COUNT(CASE WHEN averageScore >= 80 THEN 1 END) AS highly_rated_count,
    COUNT(CASE WHEN popularity >= 500000 THEN 1 END) AS very_popular_count,
    MAX(snapshot_date) AS last_updated
FROM vw_anime_current;

-- ------------------------------------------------------------------------------
-- View: Genre Statistics
-- ------------------------------------------------------------------------------
-- Statistics by genre
-- ------------------------------------------------------------------------------

CREATE OR REPLACE VIEW vw_genre_statistics AS
SELECT
    genre_name,
    COUNT(*) AS anime_count,
    AVG(averageScore) AS avg_score,
    AVG(popularity) AS avg_popularity,
    MAX(averageScore) AS top_score,
    STRING_AGG(
        title,
        ', '
        ORDER BY averageScore DESC
    ) FILTER (WHERE row_num <= 3) AS top_3_anime
FROM (
    SELECT
        genre_name,
        title,
        averageScore,
        popularity,
        ROW_NUMBER() OVER (PARTITION BY genre_name ORDER BY averageScore DESC) AS row_num
    FROM vw_anime_by_genre_expanded
) subq
GROUP BY genre_name
ORDER BY anime_count DESC;

-- ------------------------------------------------------------------------------
-- View: Anime Metrics History
-- ------------------------------------------------------------------------------
-- Historical tracking of anime metrics over time
-- ------------------------------------------------------------------------------

CREATE OR REPLACE VIEW vw_anime_metrics_history AS
SELECT
    a.anime_key,
    a.anime_id,
    a.title,
    m.snapshot_date,
    m.average_score,
    m.popularity,
    m.favorites,
    m.trending_rank,
    m.score_percentile,
    m.popularity_rank,
    -- Calculate changes from previous snapshot
    m.average_score - LAG(m.average_score) OVER w AS score_change,
    m.popularity - LAG(m.popularity) OVER w AS popularity_change,
    m.favorites - LAG(m.favorites) OVER w AS favorites_change,
    -- Days since last update
    m.snapshot_date - LAG(m.snapshot_date) OVER w AS days_since_update
FROM dim_anime a
JOIN fact_anime_metrics m ON a.anime_key = m.anime_key
WHERE a.is_current = TRUE
WINDOW w AS (PARTITION BY a.anime_key ORDER BY m.snapshot_date)
ORDER BY a.title, m.snapshot_date DESC;

-- ==============================================================================
-- MATERIALIZED VIEWS (For Performance)
-- ==============================================================================
-- Note: DuckDB supports views but not true materialized views in the same way
-- as PostgreSQL. These are regular views that can be manually refreshed.
-- ==============================================================================

-- ==============================================================================
-- VIEW DESCRIPTIONS AND DOCUMENTATION
-- ==============================================================================

COMMENT ON VIEW vw_anime_current IS 'Primary view for current anime with all dimensions and latest metrics. Used by Streamlit app.';
COMMENT ON VIEW vw_top_rated_anime IS 'Anime ranked by average score (highest first)';
COMMENT ON VIEW vw_most_popular_anime IS 'Anime ranked by popularity count (highest first)';
COMMENT ON VIEW vw_anime_recommendations IS 'Pre-computed top 10 recommendations for each anime based on similarity scores';
COMMENT ON VIEW vw_anime_by_genre_expanded IS 'Anime filtered and expanded by genre for genre-based queries';
COMMENT ON VIEW vw_anime_statistics IS 'Overall statistics about the anime catalog';
COMMENT ON VIEW vw_genre_statistics IS 'Statistics and analytics by genre';
COMMENT ON VIEW vw_anime_metrics_history IS 'Historical tracking of anime metrics with change calculations';

-- ==============================================================================
-- END OF SCRIPT
-- ==============================================================================

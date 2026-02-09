-- ==============================================================================
-- FACT TABLES FOR ANIME DATA WAREHOUSE
-- ==============================================================================
-- This script creates fact tables that store measurable metrics and events.
-- Fact tables are at the center of the star schema and contain quantitative data.
-- ==============================================================================

-- ------------------------------------------------------------------------------
-- Fact: Anime Metrics (Time-Series)
-- ------------------------------------------------------------------------------
-- Stores time-series metrics for anime (scores, popularity, etc.)
-- Each row represents a snapshot of an anime's metrics at a specific date
-- ------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS fact_anime_metrics (
    metric_key INTEGER PRIMARY KEY,
    anime_key INTEGER NOT NULL,
    format_key INTEGER,

    -- Measurable metrics (facts)
    average_score DECIMAL(5,2),                 -- 0.00 to 100.00
    popularity INTEGER,
    episodes INTEGER,
    duration_minutes INTEGER,
    favorites INTEGER,
    trending_rank INTEGER,

    -- Time dimension (snapshot date)
    snapshot_date DATE NOT NULL,

    -- Calculated/derived metrics
    score_percentile DECIMAL(5,2),              -- Percentile ranking (0-100)
    popularity_rank INTEGER,

    -- Audit columns
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    -- Foreign keys
    FOREIGN KEY (anime_key) REFERENCES dim_anime(anime_key),
    FOREIGN KEY (format_key) REFERENCES dim_format(format_key)
);

-- Create indexes for query performance
CREATE INDEX IF NOT EXISTS idx_fact_anime_key ON fact_anime_metrics(anime_key);
CREATE INDEX IF NOT EXISTS idx_fact_snapshot_date ON fact_anime_metrics(snapshot_date);
CREATE INDEX IF NOT EXISTS idx_fact_format_key ON fact_anime_metrics(format_key);
CREATE INDEX IF NOT EXISTS idx_fact_score ON fact_anime_metrics(average_score DESC);
CREATE INDEX IF NOT EXISTS idx_fact_popularity ON fact_anime_metrics(popularity DESC);

-- Composite index for common queries (anime + date)
CREATE INDEX IF NOT EXISTS idx_fact_anime_date ON fact_anime_metrics(anime_key, snapshot_date DESC);

-- ------------------------------------------------------------------------------
-- Fact: Anime Similarity Matrix
-- ------------------------------------------------------------------------------
-- Pre-computed similarity scores between anime for recommendation engine
-- Stores cosine similarity scores from TF-IDF analysis
-- ------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS fact_anime_similarity (
    anime_key_1 INTEGER NOT NULL,
    anime_key_2 INTEGER NOT NULL,
    similarity_score DECIMAL(5,4),              -- Cosine similarity (0.0000 to 1.0000)
    method VARCHAR DEFAULT 'tfidf_cosine',      -- Similarity computation method
    computed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (anime_key_1, anime_key_2),

    -- Foreign keys
    FOREIGN KEY (anime_key_1) REFERENCES dim_anime(anime_key),
    FOREIGN KEY (anime_key_2) REFERENCES dim_anime(anime_key),

    -- Constraint: anime cannot be similar to itself
    CHECK (anime_key_1 != anime_key_2)
);

-- Create indexes for fast recommendation queries
CREATE INDEX IF NOT EXISTS idx_similarity_anime1 ON fact_anime_similarity(anime_key_1, similarity_score DESC);
CREATE INDEX IF NOT EXISTS idx_similarity_anime2 ON fact_anime_similarity(anime_key_2, similarity_score DESC);
CREATE INDEX IF NOT EXISTS idx_similarity_score ON fact_anime_similarity(similarity_score DESC);
CREATE INDEX IF NOT EXISTS idx_similarity_method ON fact_anime_similarity(method);

-- ==============================================================================
-- FACT TABLE VIEWS
-- ==============================================================================

-- View for latest anime metrics (most recent snapshot)
CREATE OR REPLACE VIEW vw_fact_anime_metrics_latest AS
SELECT
    f.metric_key,
    f.anime_key,
    a.anime_id,
    a.title,
    f.average_score,
    f.popularity,
    f.episodes,
    f.duration_minutes,
    f.favorites,
    f.trending_rank,
    f.score_percentile,
    f.popularity_rank,
    f.snapshot_date,
    fmt.format_name
FROM fact_anime_metrics f
JOIN dim_anime a ON f.anime_key = a.anime_key
LEFT JOIN dim_format fmt ON f.format_key = fmt.format_key
WHERE f.snapshot_date = (
    SELECT MAX(snapshot_date)
    FROM fact_anime_metrics
    WHERE anime_key = f.anime_key
)
AND a.is_current = TRUE;

-- View for top recommendations by anime
CREATE OR REPLACE VIEW vw_top_recommendations AS
SELECT
    s.anime_key_1,
    a1.title AS source_anime_title,
    s.anime_key_2,
    a2.title AS recommended_anime_title,
    s.similarity_score,
    ROW_NUMBER() OVER (
        PARTITION BY s.anime_key_1
        ORDER BY s.similarity_score DESC
    ) AS recommendation_rank
FROM fact_anime_similarity s
JOIN dim_anime a1 ON s.anime_key_1 = a1.anime_key
JOIN dim_anime a2 ON s.anime_key_2 = a2.anime_key
WHERE a1.is_current = TRUE
  AND a2.is_current = TRUE;

-- View for anime metrics trends (historical comparison)
CREATE OR REPLACE VIEW vw_anime_metrics_trends AS
SELECT
    anime_key,
    snapshot_date,
    average_score,
    popularity,
    LAG(average_score) OVER (PARTITION BY anime_key ORDER BY snapshot_date) AS prev_score,
    LAG(popularity) OVER (PARTITION BY anime_key ORDER BY snapshot_date) AS prev_popularity,
    (average_score - LAG(average_score) OVER (PARTITION BY anime_key ORDER BY snapshot_date)) AS score_change,
    (popularity - LAG(popularity) OVER (PARTITION BY anime_key ORDER BY snapshot_date)) AS popularity_change
FROM fact_anime_metrics
ORDER BY anime_key, snapshot_date DESC;

-- ==============================================================================
-- END OF SCRIPT
-- ==============================================================================

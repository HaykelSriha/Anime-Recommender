-- Schema migration: User Ratings for Collaborative Filtering
-- Phase 2: Creates dim_user and fact_user_rating tables

-- User dimension table
-- Tracks users from MyAnimeList and other sources with cohort assignment for A/B testing
CREATE TABLE IF NOT EXISTS dim_user (
    user_key INTEGER PRIMARY KEY,
    user_id INTEGER NOT NULL,
    username VARCHAR,
    source VARCHAR NOT NULL,              -- 'myanimelist', 'kitsu', etc.
    is_test BOOLEAN DEFAULT FALSE,
    cohort_id VARCHAR,                    -- 'control', 'treatment_a', 'treatment_b'
    cohort_assigned_date DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_user_source ON dim_user(source, user_id);
CREATE INDEX IF NOT EXISTS idx_user_cohort ON dim_user(cohort_id);
CREATE INDEX IF NOT EXISTS idx_user_test ON dim_user(is_test);

-- User rating fact table (10M+ rows)
-- Core collaborative filtering data: who rated what and how much
CREATE TABLE IF NOT EXISTS fact_user_rating (
    rating_key INTEGER PRIMARY KEY,
    user_key INTEGER NOT NULL,
    anime_key INTEGER NOT NULL,
    rating FLOAT NOT NULL,                -- 0.0-5.0 scale (normalized)
    reviewed_date TIMESTAMP,
    review_text VARCHAR,                  -- Optional: for sentiment analysis
    rating_source VARCHAR,                -- 'myanimelist', 'user_input', etc.
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    FOREIGN KEY (user_key) REFERENCES dim_user(user_key),
    FOREIGN KEY (anime_key) REFERENCES dim_anime(anime_key),
    UNIQUE(user_key, anime_key)           -- One rating per user per anime
);

CREATE INDEX IF NOT EXISTS idx_rating_user ON fact_user_rating(user_key);
CREATE INDEX IF NOT EXISTS idx_rating_anime ON fact_user_rating(anime_key);
CREATE INDEX IF NOT EXISTS idx_rating_score ON fact_user_rating(rating DESC);
CREATE INDEX IF NOT EXISTS idx_rating_date ON fact_user_rating(reviewed_date DESC);

-- Data quality tracking for Phase 2
CREATE TABLE IF NOT EXISTS etl_phase2_stats (
    stat_date DATE,
    users_extracted INTEGER,
    ratings_extracted INTEGER,
    avg_rating FLOAT,
    rating_distribution VARCHAR,          -- JSON: {1: count, 2: count, ...}
    data_quality_issues VARCHAR,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- View: User activity summary (for analysis)
CREATE OR REPLACE VIEW vw_user_activity AS
SELECT
    u.user_key,
    u.username,
    u.source,
    u.cohort_id,
    COUNT(DISTINCT r.anime_key) as anime_rated,
    COUNT(*) as total_ratings,
    AVG(r.rating) as avg_rating,
    MIN(r.rating) as min_rating,
    MAX(r.rating) as max_rating,
    MAX(r.reviewed_date) as last_rating_date
FROM
    dim_user u
LEFT JOIN
    fact_user_rating r ON u.user_key = r.user_key
GROUP BY
    u.user_key, u.username, u.source, u.cohort_id
ORDER BY
    total_ratings DESC;

-- View: Anime popularity from ratings
CREATE OR REPLACE VIEW vw_anime_rating_stats AS
SELECT
    a.anime_key,
    a.anime_id,
    a.title,
    COUNT(DISTINCT r.user_key) as num_ratings,
    AVG(r.rating) as avg_rating,
    STDDEV(r.rating) as rating_stddev
FROM
    dim_anime a
LEFT JOIN
    fact_user_rating r ON a.anime_key = r.anime_key
GROUP BY
    a.anime_key, a.anime_id, a.title
ORDER BY
    num_ratings DESC;

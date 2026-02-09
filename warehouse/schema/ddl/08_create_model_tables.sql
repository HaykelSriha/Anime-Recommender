-- Phase 3 Schema: Model Storage Tables

CREATE TABLE IF NOT EXISTS fact_anime_similarity (
    similarity_key INTEGER PRIMARY KEY,
    source_anime_key INTEGER NOT NULL,
    target_anime_key INTEGER NOT NULL,
    similarity_score FLOAT NOT NULL,
    method VARCHAR NOT NULL,                -- 'tfidf', 'semantic', 'collaborative'
    rank INTEGER,                           -- 1-10 ranking
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    FOREIGN KEY (source_anime_key) REFERENCES dim_anime(anime_key),
    FOREIGN KEY (target_anime_key) REFERENCES dim_anime(anime_key)
);

CREATE INDEX IF NOT EXISTS idx_similarity_source ON fact_anime_similarity(source_anime_key);
CREATE INDEX IF NOT EXISTS idx_similarity_method ON fact_anime_similarity(method);
CREATE INDEX IF NOT EXISTS idx_similarity_score ON fact_anime_similarity(similarity_score DESC);

CREATE TABLE IF NOT EXISTS fact_collaborative_scores (
    collab_key INTEGER PRIMARY KEY,
    user_key INTEGER NOT NULL,
    anime_key INTEGER NOT NULL,
    predicted_rating FLOAT NOT NULL,        -- 0-5 scale prediction
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    FOREIGN KEY (user_key) REFERENCES dim_user(user_key),
    FOREIGN KEY (anime_key) REFERENCES dim_anime(anime_key),
    UNIQUE(user_key, anime_key)
);

CREATE INDEX IF NOT EXISTS idx_collab_user ON fact_collaborative_scores(user_key);
CREATE INDEX IF NOT EXISTS idx_collab_anime ON fact_collaborative_scores(anime_key);
CREATE INDEX IF NOT EXISTS idx_collab_score ON fact_collaborative_scores(predicted_rating DESC);

CREATE TABLE IF NOT EXISTS dim_recommendation_model (
    model_version_id VARCHAR PRIMARY KEY,
    model_type VARCHAR NOT NULL,            -- 'tfidf', 'lightfm', 'hybrid'
    training_date TIMESTAMP,
    performance_metrics JSON,               -- precision, recall, ndcg
    is_active BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS fact_user_model_score (
    score_key INTEGER PRIMARY KEY,
    user_key INTEGER,
    anime_key INTEGER NOT NULL,
    model_version_id VARCHAR NOT NULL,
    predicted_rating FLOAT NOT NULL,
    cohort_id VARCHAR,                      -- For A/B testing

    FOREIGN KEY (model_version_id) REFERENCES dim_recommendation_model(model_version_id)
);

CREATE INDEX IF NOT EXISTS idx_model_score_version ON fact_user_model_score(model_version_id);
CREATE INDEX IF NOT EXISTS idx_model_score_cohort ON fact_user_model_score(cohort_id);

-- Migration: Add Anime Relations Support
-- Purpose: Add columns to support anime series grouping (seasons, sequels, prequels)
-- Created: 2025-02-08

-- Add new columns to dim_anime for relation tracking
ALTER TABLE dim_anime ADD COLUMN IF NOT EXISTS relations TEXT;
ALTER TABLE dim_anime ADD COLUMN IF NOT EXISTS parent_anime_id INTEGER;
ALTER TABLE dim_anime ADD COLUMN IF NOT EXISTS series_root_id INTEGER;

-- Create bridge table for anime relations
CREATE TABLE IF NOT EXISTS bridge_anime_relations (
    source_anime_id INTEGER NOT NULL,
    target_anime_id INTEGER NOT NULL,
    relation_type VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (source_anime_id, target_anime_id)
);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_dim_anime_parent_id ON dim_anime(parent_anime_id);
CREATE INDEX IF NOT EXISTS idx_dim_anime_series_root_id ON dim_anime(series_root_id);
CREATE INDEX IF NOT EXISTS idx_bridge_relations_source ON bridge_anime_relations(source_anime_id);
CREATE INDEX IF NOT EXISTS idx_bridge_relations_target ON bridge_anime_relations(target_anime_id);
CREATE INDEX IF NOT EXISTS idx_bridge_relations_type ON bridge_anime_relations(relation_type);

-- Log migration
INSERT INTO etl_pipeline_runs (
    pipeline_name,
    status,
    started_at,
    completed_at,
    records_processed,
    records_failed,
    notes
) VALUES (
    'migration_002_add_anime_relations',
    'completed',
    CURRENT_TIMESTAMP,
    CURRENT_TIMESTAMP,
    0,
    0,
    'Added relation columns and bridge table'
);

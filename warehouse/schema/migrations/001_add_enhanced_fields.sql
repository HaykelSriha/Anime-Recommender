-- ==============================================================================
-- Migration: Add Enhanced Metadata Fields to dim_anime
-- ==============================================================================
-- This migration adds new columns for tags, studios, staff, characters, etc.
-- to support enhanced similarity computation
-- ==============================================================================

-- Add enhanced metadata columns to dim_anime
ALTER TABLE dim_anime ADD COLUMN IF NOT EXISTS tags TEXT;
ALTER TABLE dim_anime ADD COLUMN IF NOT EXISTS studios TEXT;
ALTER TABLE dim_anime ADD COLUMN IF NOT EXISTS staff TEXT;
ALTER TABLE dim_anime ADD COLUMN IF NOT EXISTS characters TEXT;
ALTER TABLE dim_anime ADD COLUMN IF NOT EXISTS source VARCHAR;
ALTER TABLE dim_anime ADD COLUMN IF NOT EXISTS season VARCHAR;
ALTER TABLE dim_anime ADD COLUMN IF NOT EXISTS season_year INTEGER;
ALTER TABLE dim_anime ADD COLUMN IF NOT EXISTS duration INTEGER;
ALTER TABLE dim_anime ADD COLUMN IF NOT EXISTS favourites INTEGER;
ALTER TABLE dim_anime ADD COLUMN IF NOT EXISTS is_adult BOOLEAN;

-- Create indexes for new searchable fields
CREATE INDEX IF NOT EXISTS idx_anime_source ON dim_anime(source);
CREATE INDEX IF NOT EXISTS idx_anime_season_year ON dim_anime(season_year);

-- Migration complete
SELECT 'Migration 001 completed: Enhanced metadata fields added to dim_anime' as status;

-- ==============================================================================
-- ETL METADATA TABLES FOR ANIME DATA WAREHOUSE
-- ==============================================================================
-- These tables track ETL pipeline execution, data quality checks, and data lineage.
-- They provide observability and audit capabilities for the data warehouse.
-- ==============================================================================

-- ------------------------------------------------------------------------------
-- ETL Pipeline Runs
-- ------------------------------------------------------------------------------
-- Tracks each ETL pipeline execution with metrics and status
-- ------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS etl_pipeline_runs (
    run_id INTEGER PRIMARY KEY,
    pipeline_name VARCHAR NOT NULL,
    dag_id VARCHAR,                             -- Airflow DAG ID
    task_id VARCHAR,                            -- Airflow Task ID
    run_date DATE NOT NULL,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP,
    status VARCHAR CHECK (status IN ('running', 'success', 'failed', 'skipped')),

    -- Metrics
    records_extracted INTEGER,
    records_transformed INTEGER,
    records_loaded INTEGER,
    records_rejected INTEGER,

    -- Error information
    error_message TEXT,
    error_stack_trace TEXT,

    -- Additional metadata
    execution_config JSON,                      -- Pipeline configuration
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_pipeline_runs_date ON etl_pipeline_runs(run_date DESC);
CREATE INDEX IF NOT EXISTS idx_pipeline_runs_status ON etl_pipeline_runs(status);
CREATE INDEX IF NOT EXISTS idx_pipeline_runs_name ON etl_pipeline_runs(pipeline_name);
CREATE INDEX IF NOT EXISTS idx_pipeline_runs_dag ON etl_pipeline_runs(dag_id);

-- ------------------------------------------------------------------------------
-- ETL Data Quality Checks
-- ------------------------------------------------------------------------------
-- Stores results of data quality validation checks
-- ------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS etl_data_quality_checks (
    check_id INTEGER PRIMARY KEY,
    run_id INTEGER,
    check_name VARCHAR NOT NULL,
    check_type VARCHAR,                         -- 'schema', 'null', 'duplicate', 'range', 'custom'
    table_name VARCHAR,
    column_name VARCHAR,

    -- Check results
    expected_value VARCHAR,
    actual_value VARCHAR,
    status VARCHAR CHECK (status IN ('passed', 'failed', 'warning', 'skipped')),
    severity VARCHAR CHECK (severity IN ('critical', 'warning', 'info')),

    -- Error details
    error_message TEXT,
    records_affected INTEGER,

    -- Timing
    checked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    check_duration_ms INTEGER,

    -- Foreign key
    FOREIGN KEY (run_id) REFERENCES etl_pipeline_runs(run_id)
);

CREATE INDEX IF NOT EXISTS idx_quality_checks_run ON etl_data_quality_checks(run_id);
CREATE INDEX IF NOT EXISTS idx_quality_checks_status ON etl_data_quality_checks(status);
CREATE INDEX IF NOT EXISTS idx_quality_checks_table ON etl_data_quality_checks(table_name);
CREATE INDEX IF NOT EXISTS idx_quality_checks_severity ON etl_data_quality_checks(severity);

-- ------------------------------------------------------------------------------
-- Data Quality Rules Configuration
-- ------------------------------------------------------------------------------
-- Defines the data quality rules to be executed
-- ------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS data_quality_rules (
    rule_id INTEGER PRIMARY KEY,
    rule_name VARCHAR NOT NULL UNIQUE,
    table_name VARCHAR NOT NULL,
    column_name VARCHAR,
    rule_type VARCHAR NOT NULL,                 -- 'not_null', 'unique', 'range', 'regex', 'custom_sql'
    rule_definition TEXT,                       -- SQL query or validation logic
    severity VARCHAR CHECK (severity IN ('critical', 'warning', 'info')) DEFAULT 'warning',
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_quality_rules_table ON data_quality_rules(table_name);
CREATE INDEX IF NOT EXISTS idx_quality_rules_active ON data_quality_rules(is_active);

-- ------------------------------------------------------------------------------
-- ETL Data Lineage
-- ------------------------------------------------------------------------------
-- Tracks data flow from source to target for audit and troubleshooting
-- ------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS etl_data_lineage (
    lineage_id INTEGER PRIMARY KEY,
    run_id INTEGER,
    source_system VARCHAR,                      -- 'AniList API', 'CSV', etc.
    source_entity VARCHAR,                      -- Source table/endpoint name
    target_table VARCHAR,
    target_column VARCHAR,
    transformation_logic TEXT,                  -- Description of transformation applied
    record_count INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    FOREIGN KEY (run_id) REFERENCES etl_pipeline_runs(run_id)
);

CREATE INDEX IF NOT EXISTS idx_lineage_run ON etl_data_lineage(run_id);
CREATE INDEX IF NOT EXISTS idx_lineage_target ON etl_data_lineage(target_table);
CREATE INDEX IF NOT EXISTS idx_lineage_source ON etl_data_lineage(source_system);

-- ==============================================================================
-- SEED DATA QUALITY RULES
-- ==============================================================================

-- Insert default data quality rules
INSERT INTO data_quality_rules (rule_id, rule_name, table_name, column_name, rule_type, rule_definition, severity) VALUES
    (1, 'anime_id_not_null', 'dim_anime', 'anime_id', 'not_null', 'anime_id IS NOT NULL', 'critical'),
    (2, 'anime_title_not_null', 'dim_anime', 'title', 'not_null', 'title IS NOT NULL', 'critical'),
    (3, 'average_score_range', 'fact_anime_metrics', 'average_score', 'range', 'average_score BETWEEN 0 AND 100', 'critical'),
    (4, 'popularity_positive', 'fact_anime_metrics', 'popularity', 'range', 'popularity >= 0', 'warning'),
    (5, 'episodes_positive', 'fact_anime_metrics', 'episodes', 'range', 'episodes IS NULL OR episodes > 0', 'warning'),
    (6, 'no_duplicate_current_anime', 'dim_anime', 'anime_id', 'custom_sql',
        'SELECT anime_id, COUNT(*) as cnt FROM dim_anime WHERE is_current = TRUE GROUP BY anime_id HAVING cnt > 1',
        'critical'),
    (7, 'similarity_score_range', 'fact_anime_similarity', 'similarity_score', 'range', 'similarity_score BETWEEN 0 AND 1', 'critical')
ON CONFLICT (rule_name) DO NOTHING;

-- ==============================================================================
-- METADATA VIEWS
-- ==============================================================================

-- View: ETL Pipeline Health Dashboard
CREATE OR REPLACE VIEW vw_etl_pipeline_health AS
SELECT
    pipeline_name,
    run_date,
    status,
    records_extracted,
    records_transformed,
    records_loaded,
    records_rejected,
    EXTRACT(EPOCH FROM (end_time - start_time)) AS duration_seconds,
    error_message
FROM etl_pipeline_runs
WHERE run_date >= CURRENT_DATE - INTERVAL '30 days'
ORDER BY run_date DESC, pipeline_name;

-- View: Data Quality Score by Table
CREATE OR REPLACE VIEW vw_data_quality_scores AS
SELECT
    table_name,
    COUNT(*) AS total_checks,
    SUM(CASE WHEN status = 'passed' THEN 1 ELSE 0 END) AS passed_checks,
    SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS failed_checks,
    SUM(CASE WHEN status = 'warning' THEN 1 ELSE 0 END) AS warning_checks,
    ROUND(100.0 * SUM(CASE WHEN status = 'passed' THEN 1 ELSE 0 END) / COUNT(*), 2) AS quality_score_pct,
    MAX(checked_at) AS last_check_time
FROM etl_data_quality_checks
WHERE checked_at >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY table_name
ORDER BY quality_score_pct ASC;

-- View: Recent Data Quality Issues
CREATE OR REPLACE VIEW vw_recent_quality_issues AS
SELECT
    q.checked_at,
    q.check_name,
    q.table_name,
    q.column_name,
    q.status,
    q.severity,
    q.error_message,
    q.records_affected,
    p.pipeline_name,
    p.run_date
FROM etl_data_quality_checks q
JOIN etl_pipeline_runs p ON q.run_id = p.run_id
WHERE q.status IN ('failed', 'warning')
  AND q.checked_at >= CURRENT_DATE - INTERVAL '7 days'
ORDER BY q.checked_at DESC, q.severity DESC;

-- View: Pipeline Success Rate (Last 30 days)
CREATE OR REPLACE VIEW vw_pipeline_success_rate AS
SELECT
    pipeline_name,
    COUNT(*) AS total_runs,
    SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) AS successful_runs,
    SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS failed_runs,
    ROUND(100.0 * SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) / COUNT(*), 2) AS success_rate_pct,
    AVG(EXTRACT(EPOCH FROM (end_time - start_time))) AS avg_duration_seconds
FROM etl_pipeline_runs
WHERE run_date >= CURRENT_DATE - INTERVAL '30 days'
  AND status IN ('success', 'failed')
GROUP BY pipeline_name
ORDER BY success_rate_pct ASC;

-- ==============================================================================
-- END OF SCRIPT
-- ==============================================================================

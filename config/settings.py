"""
Central Configuration for Anime Recommender Data Warehouse
===========================================================
This module provides centralized configuration management for all components
of the anime recommender data warehouse project.
"""

import os
from pathlib import Path
from typing import Dict, Any
import yaml

# ==============================================================================
# BASE DIRECTORIES
# ==============================================================================

# Project root directory
BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / 'data'
WAREHOUSE_DIR = BASE_DIR / 'warehouse'
ETL_DIR = BASE_DIR / 'etl'
SCRIPTS_DIR = BASE_DIR / 'scripts'
LOG_DIR = BASE_DIR / 'logs'
CONFIG_DIR = BASE_DIR / 'config'
AIRFLOW_DIR = BASE_DIR / 'airflow'

# ==============================================================================
# DATA WAREHOUSE CONFIGURATION
# ==============================================================================

# DuckDB Database
WAREHOUSE_DB_PATH = WAREHOUSE_DIR / 'anime_dw.duckdb'
WAREHOUSE_SCHEMA_DIR = WAREHOUSE_DIR / 'schema' / 'ddl'
WAREHOUSE_BACKUP_DIR = WAREHOUSE_DIR / 'backups'

# Connection settings
WAREHOUSE_READ_ONLY = False
WAREHOUSE_MEMORY_LIMIT = '2GB'
WAREHOUSE_THREADS = 4

# ==============================================================================
# DATA SOURCES
# ==============================================================================

# AniList GraphQL API
ANILIST_API_URL = 'https://graphql.anilist.co'
ANILIST_RATE_LIMIT = 90  # requests per minute
ANILIST_PAGE_SIZE = 50
ANILIST_MAX_RETRIES = 3
ANILIST_RETRY_DELAY = 5  # seconds
ANILIST_TIMEOUT = 30  # seconds

# Legacy CSV files
CSV_RAW_PATH = DATA_DIR / 'raw' / 'anilist_top.csv'
CSV_CLEAN_PATH = DATA_DIR / 'clean' / 'anime_clean.csv'
CSV_STAGING_DIR = DATA_DIR / 'staging'
CSV_ARCHIVE_DIR = DATA_DIR / 'archive'

# ==============================================================================
# ETL CONFIGURATION
# ==============================================================================

# Batch processing
ETL_BATCH_SIZE = 100
ETL_COMMIT_FREQUENCY = 50  # Commit every N records

# Retry logic
ETL_RETRY_ATTEMPTS = 3
ETL_RETRY_DELAY_SECONDS = 5
ETL_RETRY_EXPONENTIAL_BACKOFF = True

# Data extraction
EXTRACT_TOP_ANIME_LIMIT = 1000
EXTRACT_INCREMENTAL_HOURS = 24  # Look back 24 hours for incremental

# Data transformation
TRANSFORM_REMOVE_HTML = True
TRANSFORM_NORMALIZE_GENRES = True
TRANSFORM_MIN_DESCRIPTION_LENGTH = 10
TRANSFORM_MAX_DESCRIPTION_LENGTH = 10000

# ==============================================================================
# DATA QUALITY CONFIGURATION
# ==============================================================================

# Quality checks
QUALITY_CHECK_ENABLED = True
QUALITY_FAIL_ON_CRITICAL = True
QUALITY_FAIL_ON_WARNING = False
QUALITY_MIN_SCORE_THRESHOLD = 95.0  # Minimum quality score percentage

# Quality rules file
QUALITY_RULES_FILE = CONFIG_DIR / 'data_quality_rules.yaml'

# Anomaly detection
ANOMALY_DETECTION_ENABLED = True
ANOMALY_SCORE_CHANGE_THRESHOLD = 20  # Alert if score changes by more than 20 points
ANOMALY_POPULARITY_CHANGE_THRESHOLD_PCT = 50  # Alert if popularity changes by more than 50%

# ==============================================================================
# MACHINE LEARNING / RECOMMENDATION ENGINE
# ==============================================================================

# TF-IDF Settings
TFIDF_MAX_FEATURES = 1000
TFIDF_STOP_WORDS = 'english'
TFIDF_MIN_DF = 1
TFIDF_MAX_DF = 0.8

# Similarity computation
SIMILARITY_METHOD = 'cosine'
SIMILARITY_MIN_SCORE = 0.1  # Don't store similarities below this threshold
SIMILARITY_TOP_N = 50  # Store only top N similar anime per anime

# ==============================================================================
# AIRFLOW CONFIGURATION
# ==============================================================================

# Airflow home
AIRFLOW_HOME = AIRFLOW_DIR
AIRFLOW_DAGS_DIR = AIRFLOW_DIR / 'dags'
AIRFLOW_PLUGINS_DIR = AIRFLOW_DIR / 'plugins'
AIRFLOW_LOGS_DIR = AIRFLOW_DIR / 'logs'

# DAG default settings
DAG_DEFAULT_RETRIES = 3
DAG_RETRY_DELAY_MINUTES = 5
DAG_EMAIL_ON_FAILURE = True
DAG_EMAIL_ON_RETRY = False
DAG_CATCHUP = False

# Schedule intervals
SCHEDULE_DAILY_SYNC = '0 2 * * *'  # 2 AM UTC daily
SCHEDULE_WEEKLY_FULL_REFRESH = '0 1 * * 0'  # 1 AM UTC Sunday
SCHEDULE_QUALITY_MONITOR = '0 * * * *'  # Every hour

# ==============================================================================
# LOGGING CONFIGURATION
# ==============================================================================

# Log level
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
LOG_DATE_FORMAT = '%Y-%m-%d %H:%M:%S'

# Log files
LOG_ETL_FILE = LOG_DIR / 'etl' / 'etl.log'
LOG_APP_FILE = LOG_DIR / 'app' / 'app.log'
LOG_AIRFLOW_FILE = AIRFLOW_LOGS_DIR / 'airflow.log'

# Log rotation
LOG_MAX_BYTES = 10 * 1024 * 1024  # 10 MB
LOG_BACKUP_COUNT = 5

# ==============================================================================
# APPLICATION CONFIGURATION (Streamlit)
# ==============================================================================

# Streamlit settings
APP_TITLE = 'Anime Recommender'
APP_ICON = '🎌'
APP_LAYOUT = 'wide'
APP_SIDEBAR_STATE = 'expanded'

# Cache settings
CACHE_TTL_SECONDS = 3600  # 1 hour
CACHE_RECOMMENDER = True
CACHE_IMAGES = True

# Display settings
DEFAULT_RECOMMENDATIONS_COUNT = 5
MAX_BROWSE_RESULTS = 100
CARD_IMAGE_HEIGHT_PX = 350

# ==============================================================================
# ENVIRONMENT-SPECIFIC CONFIGURATION
# ==============================================================================

# Environment detection
ENVIRONMENT = os.getenv('ENVIRONMENT', 'development')  # development, staging, production

# Development overrides
if ENVIRONMENT == 'development':
    LOG_LEVEL = 'DEBUG'
    ETL_BATCH_SIZE = 10
    EXTRACT_TOP_ANIME_LIMIT = 100

# Production overrides
elif ENVIRONMENT == 'production':
    LOG_LEVEL = 'INFO'
    WAREHOUSE_READ_ONLY = False
    QUALITY_FAIL_ON_CRITICAL = True

# ==============================================================================
# HELPER FUNCTIONS
# ==============================================================================

def load_yaml_config(config_file: str) -> Dict[str, Any]:
    """
    Load configuration from YAML file

    Args:
        config_file: Name of the config file (relative to config directory)

    Returns:
        Dictionary containing configuration
    """
    config_path = CONFIG_DIR / config_file
    if not config_path.exists():
        raise FileNotFoundError(f"Configuration file not found: {config_path}")

    with open(config_path, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)


def ensure_directories():
    """
    Create all necessary directories if they don't exist
    """
    directories = [
        DATA_DIR,
        DATA_DIR / 'raw',
        DATA_DIR / 'clean',
        DATA_DIR / 'staging',
        DATA_DIR / 'archive',
        WAREHOUSE_DIR,
        WAREHOUSE_DIR / 'schema' / 'ddl',
        WAREHOUSE_DIR / 'backups',
        LOG_DIR / 'etl',
        LOG_DIR / 'app',
        AIRFLOW_LOGS_DIR,
        SCRIPTS_DIR,
        ETL_DIR,
    ]

    for directory in directories:
        directory.mkdir(parents=True, exist_ok=True)


def get_database_connection_string() -> str:
    """
    Get DuckDB connection string

    Returns:
        Database connection string
    """
    return str(WAREHOUSE_DB_PATH)


def validate_configuration():
    """
    Validate that all required configuration is present and valid

    Raises:
        ValueError: If configuration is invalid
    """
    errors = []

    # Check required directories
    if not BASE_DIR.exists():
        errors.append(f"Base directory does not exist: {BASE_DIR}")

    # Check API configuration
    if not ANILIST_API_URL:
        errors.append("AniList API URL not configured")

    if ANILIST_RATE_LIMIT <= 0:
        errors.append(f"Invalid rate limit: {ANILIST_RATE_LIMIT}")

    # Check batch sizes
    if ETL_BATCH_SIZE <= 0:
        errors.append(f"Invalid batch size: {ETL_BATCH_SIZE}")

    # Check quality thresholds
    if not (0 <= QUALITY_MIN_SCORE_THRESHOLD <= 100):
        errors.append(f"Invalid quality threshold: {QUALITY_MIN_SCORE_THRESHOLD}")

    if errors:
        raise ValueError("Configuration validation failed:\n" + "\n".join(errors))


# ==============================================================================
# INITIALIZATION
# ==============================================================================

# Ensure directories exist when module is imported
try:
    ensure_directories()
except OSError:
    pass  # Read-only filesystem (e.g., Streamlit Cloud)

# Validate configuration
try:
    validate_configuration()
except ValueError as e:
    print(f"Warning: {e}")

# ==============================================================================
# EXPORT ALL SETTINGS
# ==============================================================================

__all__ = [
    'BASE_DIR',
    'DATA_DIR',
    'WAREHOUSE_DIR',
    'ETL_DIR',
    'SCRIPTS_DIR',
    'LOG_DIR',
    'CONFIG_DIR',
    'AIRFLOW_DIR',
    'WAREHOUSE_DB_PATH',
    'ANILIST_API_URL',
    'ANILIST_RATE_LIMIT',
    'ETL_BATCH_SIZE',
    'QUALITY_CHECK_ENABLED',
    'load_yaml_config',
    'ensure_directories',
    'get_database_connection_string',
    'validate_configuration',
]

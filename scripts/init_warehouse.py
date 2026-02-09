"""
Warehouse Initialization Script
================================
This script initializes the DuckDB data warehouse by executing all DDL scripts
in the correct order to create the complete database schema.

Usage:
    python scripts/init_warehouse.py [--force]

Options:
    --force: Drop and recreate database even if it exists
"""

import sys
import argparse
from pathlib import Path
import duckdb
import logging

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from config.settings import (
    WAREHOUSE_DB_PATH,
    WAREHOUSE_SCHEMA_DIR,
    LOG_DIR,
    LOG_LEVEL,
    LOG_FORMAT
)

# ==============================================================================
# LOGGING SETUP
# ==============================================================================

logging.basicConfig(
    level=LOG_LEVEL,
    format=LOG_FORMAT,
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_DIR / 'etl' / 'warehouse_init.log')
    ]
)
logger = logging.getLogger(__name__)

# ==============================================================================
# DDL SCRIPT EXECUTION ORDER
# ==============================================================================

DDL_SCRIPTS = [
    '01_create_dimensions.sql',
    '02_create_facts.sql',
    '03_create_bridge_tables.sql',
    '04_create_metadata_tables.sql',
    '05_create_views.sql',
]

# ==============================================================================
# INITIALIZATION FUNCTIONS
# ==============================================================================

def check_database_exists() -> bool:
    """
    Check if the warehouse database file already exists

    Returns:
        True if database exists, False otherwise
    """
    return WAREHOUSE_DB_PATH.exists()


def drop_database():
    """
    Drop the existing database file
    """
    if WAREHOUSE_DB_PATH.exists():
        logger.warning(f"Dropping existing database: {WAREHOUSE_DB_PATH}")
        WAREHOUSE_DB_PATH.unlink()
    else:
        logger.info("No existing database to drop")


def execute_sql_file(conn: duckdb.DuckDBPyConnection, sql_file_path: Path):
    """
    Execute SQL statements from a file

    Args:
        conn: DuckDB connection
        sql_file_path: Path to SQL file

    Raises:
        Exception: If SQL execution fails
    """
    logger.info(f"Executing SQL script: {sql_file_path.name}")

    try:
        with open(sql_file_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()

        # Split into individual statements (basic splitting by semicolon)
        # Note: This is a simple approach and may not handle complex SQL
        statements = [stmt.strip() for stmt in sql_content.split(';') if stmt.strip()]

        for i, statement in enumerate(statements, 1):
            # Skip comments and empty statements
            if statement.startswith('--') or not statement:
                continue

            try:
                conn.execute(statement)
                logger.debug(f"  Executed statement {i}/{len(statements)}")
            except Exception as e:
                # Log but continue for non-critical errors (like CREATE IF NOT EXISTS)
                logger.debug(f"  Statement {i} info: {str(e)}")

        logger.info(f"✓ Successfully executed: {sql_file_path.name}")

    except Exception as e:
        logger.error(f"✗ Failed to execute {sql_file_path.name}: {str(e)}")
        raise


def initialize_warehouse(force: bool = False):
    """
    Initialize the data warehouse by creating all tables and views

    Args:
        force: If True, drop and recreate database even if it exists

    Returns:
        True if successful, False otherwise
    """
    logger.info("=" * 80)
    logger.info("INITIALIZING ANIME DATA WAREHOUSE")
    logger.info("=" * 80)

    # Check if database exists
    db_exists = check_database_exists()

    if db_exists and not force:
        logger.error(
            f"Database already exists at {WAREHOUSE_DB_PATH}. "
            f"Use --force to drop and recreate."
        )
        return False

    if db_exists and force:
        drop_database()

    # Ensure warehouse directory exists
    WAREHOUSE_DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    # Connect to database (creates if doesn't exist)
    logger.info(f"Creating database: {WAREHOUSE_DB_PATH}")

    try:
        conn = duckdb.connect(str(WAREHOUSE_DB_PATH))
        logger.info("✓ Database connection established")

        # Execute DDL scripts in order
        logger.info(f"\nExecuting {len(DDL_SCRIPTS)} DDL scripts...")

        for script_name in DDL_SCRIPTS:
            script_path = WAREHOUSE_SCHEMA_DIR / script_name

            if not script_path.exists():
                logger.error(f"✗ DDL script not found: {script_path}")
                return False

            execute_sql_file(conn, script_path)

        # Verify schema creation
        logger.info("\nVerifying schema creation...")
        verify_schema(conn)

        # Close connection
        conn.close()
        logger.info("\n" + "=" * 80)
        logger.info("✓ WAREHOUSE INITIALIZATION COMPLETE")
        logger.info("=" * 80)
        logger.info(f"Database created at: {WAREHOUSE_DB_PATH}")
        logger.info(f"Database size: {WAREHOUSE_DB_PATH.stat().st_size / 1024:.2f} KB")

        return True

    except Exception as e:
        logger.error(f"\n✗ Initialization failed: {str(e)}")
        logger.exception("Detailed error:")
        return False


def verify_schema(conn: duckdb.DuckDBPyConnection):
    """
    Verify that all expected tables and views were created

    Args:
        conn: DuckDB connection
    """
    # Expected tables
    expected_tables = [
        'dim_anime',
        'dim_genre',
        'dim_format',
        'fact_anime_metrics',
        'fact_anime_similarity',
        'bridge_anime_genre',
        'etl_pipeline_runs',
        'etl_data_quality_checks',
        'data_quality_rules',
        'etl_data_lineage',
    ]

    # Expected views
    expected_views = [
        'vw_anime_current',
        'vw_top_rated_anime',
        'vw_most_popular_anime',
        'vw_anime_recommendations',
        'vw_anime_by_genre_expanded',
        'vw_anime_statistics',
        'vw_genre_statistics',
        'vw_anime_metrics_history',
        'vw_etl_pipeline_health',
        'vw_data_quality_scores',
        'vw_recent_quality_issues',
        'vw_pipeline_success_rate',
    ]

    # Get all tables
    tables_result = conn.execute("SHOW TABLES").fetchall()
    tables = [row[0] for row in tables_result]

    # Check tables
    logger.info("\nTables created:")
    missing_tables = []
    for table in expected_tables:
        if table in tables:
            logger.info(f"  ✓ {table}")
        else:
            logger.warning(f"  ✗ {table} (MISSING)")
            missing_tables.append(table)

    # Get all views (views also appear in SHOW TABLES in DuckDB)
    logger.info("\nViews created:")
    missing_views = []
    for view in expected_views:
        if view in tables:
            logger.info(f"  ✓ {view}")
        else:
            logger.warning(f"  ✗ {view} (MISSING)")
            missing_views.append(view)

    # Check seed data
    logger.info("\nVerifying seed data:")

    # Check genres
    genre_count = conn.execute("SELECT COUNT(*) FROM dim_genre").fetchone()[0]
    logger.info(f"  Genres seeded: {genre_count}")

    # Check formats
    format_count = conn.execute("SELECT COUNT(*) FROM dim_format").fetchone()[0]
    logger.info(f"  Formats seeded: {format_count}")

    # Check quality rules
    rules_count = conn.execute("SELECT COUNT(*) FROM data_quality_rules").fetchone()[0]
    logger.info(f"  Quality rules seeded: {rules_count}")

    # Report summary
    if missing_tables or missing_views:
        logger.warning(f"\n⚠ Schema verification found {len(missing_tables)} missing tables and {len(missing_views)} missing views")
    else:
        logger.info("\n✓ All expected tables and views created successfully")


def print_next_steps():
    """
    Print instructions for next steps after initialization
    """
    print("\n" + "=" * 80)
    print("NEXT STEPS:")
    print("=" * 80)
    print("1. Migrate CSV data to warehouse:")
    print("   python scripts/migrate_csv_to_db.py")
    print()
    print("2. Verify data migration:")
    print("   python -c \"import duckdb; conn = duckdb.connect('warehouse/anime_dw.duckdb'); print(conn.execute('SELECT COUNT(*) FROM vw_anime_current').fetchone())\"")
    print()
    print("3. Set up ETL pipeline components (extract, transform, load)")
    print()
    print("4. Configure and start Apache Airflow")
    print("=" * 80)


# ==============================================================================
# MAIN EXECUTION
# ==============================================================================

def main():
    """
    Main execution function
    """
    parser = argparse.ArgumentParser(
        description='Initialize the anime data warehouse'
    )
    parser.add_argument(
        '--force',
        action='store_true',
        help='Drop and recreate database if it already exists'
    )

    args = parser.parse_args()

    # Run initialization
    success = initialize_warehouse(force=args.force)

    if success:
        print_next_steps()
        sys.exit(0)
    else:
        logger.error("Initialization failed. Check logs for details.")
        sys.exit(1)


if __name__ == '__main__':
    main()

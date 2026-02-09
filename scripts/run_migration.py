"""
Database Migration Script
=========================
Run schema migrations on the warehouse
"""

import sys
from pathlib import Path
import duckdb
import logging

sys.path.insert(0, str(Path(__file__).parent.parent))

from config.settings import WAREHOUSE_DB_PATH

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def run_migration(migration_file: Path):
    """
    Run a single migration file

    Args:
        migration_file: Path to SQL migration file
    """
    logger.info(f"Running migration: {migration_file.name}")

    # Read SQL
    with open(migration_file, 'r', encoding='utf-8') as f:
        sql = f.read()

    # Connect and execute
    conn = duckdb.connect(str(WAREHOUSE_DB_PATH))

    try:
        # Execute migration
        conn.execute(sql)

        logger.info(f"Migration {migration_file.name} completed successfully")

    except Exception as e:
        logger.error(f"Migration failed: {str(e)}")
        raise

    finally:
        conn.close()


def run_all_migrations():
    """Run all pending migrations"""
    migrations_dir = Path(__file__).parent.parent / 'warehouse' / 'schema' / 'migrations'

    if not migrations_dir.exists():
        logger.warning(f"Migrations directory not found: {migrations_dir}")
        return

    # Get all SQL files
    migration_files = sorted(migrations_dir.glob('*.sql'))

    if not migration_files:
        logger.info("No migrations found")
        return

    logger.info(f"Found {len(migration_files)} migration(s)")

    for migration_file in migration_files:
        run_migration(migration_file)

    logger.info("All migrations completed successfully")


if __name__ == '__main__':
    run_all_migrations()

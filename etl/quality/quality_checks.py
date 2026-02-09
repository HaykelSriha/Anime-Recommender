"""
Data Quality Checks
===================
Validates data quality across the ETL pipeline and warehouse.

Check Types:
- Schema validation
- Null checks
- Range validation
- Duplicate detection
- Referential integrity
- Custom SQL checks
"""

import duckdb
import pandas as pd
from typing import List, Dict, Any, Tuple
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


class QualityCheck:
    """Base class for quality checks"""

    def __init__(self, name: str, severity: str = 'warning'):
        self.name = name
        self.severity = severity  # 'critical', 'warning', 'info'

    def run(self, conn: duckdb.DuckDBPyConnection) -> Tuple[bool, str, Any]:
        """
        Run the check

        Returns:
            Tuple of (passed, message, actual_value)
        """
        raise NotImplementedError


class NotNullCheck(QualityCheck):
    """Check that a column has no null values"""

    def __init__(self, table: str, column: str, severity: str = 'critical'):
        super().__init__(f"{table}.{column}_not_null", severity)
        self.table = table
        self.column = column

    def run(self, conn: duckdb.DuckDBPyConnection) -> Tuple[bool, str, Any]:
        query = f"SELECT COUNT(*) FROM {self.table} WHERE {self.column} IS NULL"

        try:
            null_count = conn.execute(query).fetchone()[0]
            passed = null_count == 0
            message = f"Found {null_count} null values" if not passed else "No null values"
            return passed, message, null_count
        except Exception as e:
            return False, f"Check failed: {str(e)}", None


class RangeCheck(QualityCheck):
    """Check that a column's values are within a range"""

    def __init__(
        self,
        table: str,
        column: str,
        min_value: Any,
        max_value: Any,
        severity: str = 'critical'
    ):
        super().__init__(f"{table}.{column}_range", severity)
        self.table = table
        self.column = column
        self.min_value = min_value
        self.max_value = max_value

    def run(self, conn: duckdb.DuckDBPyConnection) -> Tuple[bool, str, Any]:
        query = f"""
            SELECT COUNT(*)
            FROM {self.table}
            WHERE {self.column} IS NOT NULL
              AND ({self.column} < {self.min_value} OR {self.column} > {self.max_value})
        """

        try:
            out_of_range = conn.execute(query).fetchone()[0]
            passed = out_of_range == 0
            message = f"Found {out_of_range} values out of range [{self.min_value}, {self.max_value}]"
            return passed, message, out_of_range
        except Exception as e:
            return False, f"Check failed: {str(e)}", None


class UniqueCheck(QualityCheck):
    """Check that a column has no duplicates"""

    def __init__(self, table: str, column: str, severity: str = 'critical'):
        super().__init__(f"{table}.{column}_unique", severity)
        self.table = table
        self.column = column

    def run(self, conn: duckdb.DuckDBPyConnection) -> Tuple[bool, str, Any]:
        query = f"""
            SELECT COUNT(*) - COUNT(DISTINCT {self.column})
            FROM {self.table}
        """

        try:
            duplicate_count = conn.execute(query).fetchone()[0]
            passed = duplicate_count == 0
            message = f"Found {duplicate_count} duplicate values"
            return passed, message, duplicate_count
        except Exception as e:
            return False, f"Check failed: {str(e)}", None


class ReferentialIntegrityCheck(QualityCheck):
    """Check foreign key relationships"""

    def __init__(
        self,
        child_table: str,
        parent_table: str,
        fk_column: str,
        pk_column: str,
        severity: str = 'critical'
    ):
        super().__init__(f"{child_table}.{fk_column}_ref_integrity", severity)
        self.child_table = child_table
        self.parent_table = parent_table
        self.fk_column = fk_column
        self.pk_column = pk_column

    def run(self, conn: duckdb.DuckDBPyConnection) -> Tuple[bool, str, Any]:
        query = f"""
            SELECT COUNT(*)
            FROM {self.child_table} c
            LEFT JOIN {self.parent_table} p ON c.{self.fk_column} = p.{self.pk_column}
            WHERE p.{self.pk_column} IS NULL AND c.{self.fk_column} IS NOT NULL
        """

        try:
            orphan_count = conn.execute(query).fetchone()[0]
            passed = orphan_count == 0
            message = f"Found {orphan_count} orphan records"
            return passed, message, orphan_count
        except Exception as e:
            return False, f"Check failed: {str(e)}", None


class CustomSQLCheck(QualityCheck):
    """Custom SQL check - query should return count of violations"""

    def __init__(self, name: str, query: str, severity: str = 'warning'):
        super().__init__(name, severity)
        self.query = query

    def run(self, conn: duckdb.DuckDBPyConnection) -> Tuple[bool, str, Any]:
        try:
            result = conn.execute(self.query).fetchall()
            violation_count = len(result)
            passed = violation_count == 0
            message = f"Found {violation_count} violations"
            return passed, message, violation_count
        except Exception as e:
            return False, f"Check failed: {str(e)}", None


class DataQualityChecker:
    """
    Manages and executes data quality checks
    """

    def __init__(self, db_path: str):
        self.db_path = db_path
        self.checks: List[QualityCheck] = []

        logger.info(f"Initialized DataQualityChecker for {db_path}")

    def add_check(self, check: QualityCheck):
        """Add a quality check"""
        self.checks.append(check)

    def add_standard_checks(self):
        """Add standard quality checks for anime warehouse"""

        # Anime dimension checks
        self.add_check(NotNullCheck('dim_anime', 'anime_id', 'critical'))
        self.add_check(NotNullCheck('dim_anime', 'title', 'critical'))
        self.add_check(UniqueCheck('dim_anime', 'anime_id', 'critical'))

        # Metrics fact checks
        self.add_check(RangeCheck('fact_anime_metrics', 'average_score', 0, 100, 'critical'))
        self.add_check(RangeCheck('fact_anime_metrics', 'popularity', 0, 999999999, 'warning'))
        self.add_check(NotNullCheck('fact_anime_metrics', 'anime_key', 'critical'))

        # Referential integrity
        self.add_check(ReferentialIntegrityCheck(
            'fact_anime_metrics', 'dim_anime', 'anime_key', 'anime_key', 'critical'
        ))

        self.add_check(ReferentialIntegrityCheck(
            'bridge_anime_genre', 'dim_anime', 'anime_key', 'anime_key', 'critical'
        ))

        self.add_check(ReferentialIntegrityCheck(
            'bridge_anime_genre', 'dim_genre', 'genre_key', 'genre_key', 'critical'
        ))

        # Custom checks
        self.add_check(CustomSQLCheck(
            'no_duplicate_current_anime',
            """
            SELECT anime_id, COUNT(*) as cnt
            FROM dim_anime
            WHERE is_current = TRUE
            GROUP BY anime_id
            HAVING cnt > 1
            """,
            'critical'
        ))

        logger.info(f"Added {len(self.checks)} standard quality checks")

    def run_all(self) -> Dict[str, Any]:
        """
        Run all quality checks

        Returns:
            Dictionary with check results
        """
        logger.info(f"Running {len(self.checks)} quality checks...")

        conn = duckdb.connect(self.db_path)

        results = {
            'total_checks': len(self.checks),
            'passed': 0,
            'failed': 0,
            'checks': []
        }

        try:
            for check in self.checks:
                logger.debug(f"Running check: {check.name}")

                try:
                    passed, message, actual_value = check.run(conn)

                    result = {
                        'name': check.name,
                        'severity': check.severity,
                        'status': 'passed' if passed else 'failed',
                        'message': message,
                        'actual_value': actual_value
                    }

                    results['checks'].append(result)

                    if passed:
                        results['passed'] += 1
                    else:
                        results['failed'] += 1
                        level = logging.ERROR if check.severity == 'critical' else logging.WARNING
                        logger.log(level, f"Check failed: {check.name} - {message}")

                except Exception as e:
                    logger.error(f"Check error: {check.name} - {str(e)}")
                    results['checks'].append({
                        'name': check.name,
                        'severity': check.severity,
                        'status': 'error',
                        'message': str(e),
                        'actual_value': None
                    })
                    results['failed'] += 1

            # Calculate quality score
            results['quality_score'] = (
                100.0 * results['passed'] / results['total_checks']
                if results['total_checks'] > 0 else 0
            )

            logger.info(
                f"Quality checks complete: {results['passed']}/{results['total_checks']} passed "
                f"(score: {results['quality_score']:.1f}%)"
            )

        finally:
            conn.close()

        return results

    def print_report(self, results: Dict[str, Any]):
        """Print quality check report"""

        print("\n" + "=" * 80)
        print("DATA QUALITY REPORT")
        print("=" * 80)
        print(f"Total Checks: {results['total_checks']}")
        print(f"Passed: {results['passed']}")
        print(f"Failed: {results['failed']}")
        print(f"Quality Score: {results['quality_score']:.1f}%")
        print("=" * 80)

        # Group by severity
        critical_failures = [c for c in results['checks'] if c['severity'] == 'critical' and c['status'] == 'failed']
        warnings = [c for c in results['checks'] if c['severity'] == 'warning' and c['status'] == 'failed']

        if critical_failures:
            print("\nCRITICAL FAILURES:")
            for check in critical_failures:
                print(f"  [CRITICAL] {check['name']}: {check['message']}")

        if warnings:
            print("\nWARNINGS:")
            for check in warnings:
                print(f"  [WARNING] {check['name']}: {check['message']}")

        if not critical_failures and not warnings:
            print("\n✓ All checks passed!")

        print("=" * 80 + "\n")


if __name__ == '__main__':
    # Test quality checks
    logging.basicConfig(level=logging.INFO)

    checker = DataQualityChecker('warehouse/anime_dw.duckdb')
    checker.add_standard_checks()

    results = checker.run_all()
    checker.print_report(results)

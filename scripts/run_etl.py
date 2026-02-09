"""
ETL Pipeline Orchestrator
==========================
Runs the complete ETL pipeline:
1. Extract data from AniList API
2. Transform and validate data
3. Load into warehouse
4. Compute similarity scores
5. Run quality checks

Usage:
    python scripts/run_etl.py [--limit LIMIT] [--incremental] [--skip-similarity]
"""

import sys
import argparse
from pathlib import Path
from datetime import datetime, timedelta
import logging

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from etl.extract.anilist_extractor import AniListExtractor
from etl.transform.anime_transformer import AnimeTransformer
from etl.load.warehouse_loader import WarehouseLoader
from etl.ml.similarity_engine import SimilarityEngine
from etl.quality.quality_checks import DataQualityChecker
from config.settings import WAREHOUSE_DB_PATH, LOG_DIR, LOG_LEVEL, LOG_FORMAT

# ==============================================================================
# LOGGING SETUP
# ==============================================================================

logging.basicConfig(
    level=LOG_LEVEL,
    format=LOG_FORMAT,
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_DIR / 'etl' / 'run_etl.log')
    ]
)
logger = logging.getLogger(__name__)

# ==============================================================================
# ETL PIPELINE
# ==============================================================================

class ETLPipeline:
    """Complete ETL pipeline orchestrator"""

    def __init__(
        self,
        db_path: str,
        limit: int = 100,
        incremental: bool = False,
        skip_similarity: bool = False,
        skip_quality: bool = False
    ):
        """
        Initialize ETL pipeline

        Args:
            db_path: Path to warehouse database
            limit: Number of anime to extract
            incremental: Use incremental extraction
            skip_similarity: Skip similarity computation
            skip_quality: Skip quality checks
        """
        self.db_path = db_path
        self.limit = limit
        self.incremental = incremental
        self.skip_similarity = skip_similarity
        self.skip_quality = skip_quality

        self.stats = {
            'start_time': None,
            'end_time': None,
            'extracted': 0,
            'transformed': 0,
            'loaded': 0,
            'similarities_computed': 0,
            'quality_score': 0
        }

    def run(self):
        """Run the complete ETL pipeline"""

        self.stats['start_time'] = datetime.now()

        logger.info("=" * 80)
        logger.info("STARTING ETL PIPELINE")
        logger.info("=" * 80)
        logger.info(f"Configuration:")
        logger.info(f"  - Database: {self.db_path}")
        logger.info(f"  - Limit: {self.limit}")
        logger.info(f"  - Incremental: {self.incremental}")
        logger.info(f"  - Skip similarity: {self.skip_similarity}")
        logger.info(f"  - Skip quality: {self.skip_quality}")
        logger.info("=" * 80)

        try:
            # Step 1: Extract
            raw_data = self._extract()

            if not raw_data:
                logger.error("No data extracted, aborting pipeline")
                return False

            # Step 2: Transform
            transformed_data = self._transform(raw_data)

            # Step 3: Load
            self._load(transformed_data)

            # Step 4: Compute similarities (optional)
            if not self.skip_similarity:
                self._compute_similarities()

            # Step 5: Quality checks (optional)
            if not self.skip_quality:
                self._run_quality_checks()

            # Summary
            self.stats['end_time'] = datetime.now()
            self._print_summary()

            logger.info("=" * 80)
            logger.info("ETL PIPELINE COMPLETE")
            logger.info("=" * 80)

            return True

        except Exception as e:
            logger.error(f"ETL Pipeline failed: {str(e)}")
            logger.exception("Detailed error:")
            return False

    def _extract(self):
        """Extract data from AniList API"""

        logger.info("\n" + "=" * 80)
        logger.info("STEP 1: EXTRACT")
        logger.info("=" * 80)

        extractor = AniListExtractor()

        if self.incremental:
            # Extract updates from last 24 hours
            since = int((datetime.now() - timedelta(hours=24)).timestamp())
            raw_data = extractor.extract_incremental(since)
        else:
            # Full extraction
            raw_data = extractor.extract_top_anime(limit=self.limit)

        self.stats['extracted'] = len(raw_data)

        logger.info(f"Extraction complete: {len(raw_data)} records")
        return raw_data

    def _transform(self, raw_data):
        """Transform extracted data"""

        logger.info("\n" + "=" * 80)
        logger.info("STEP 2: TRANSFORM")
        logger.info("=" * 80)

        transformer = AnimeTransformer(
            remove_html=True,
            normalize_genres=True,
            calculate_metrics=True
        )

        df, errors = transformer.transform(raw_data)

        if errors:
            logger.warning(f"Transformation completed with {len(errors)} validation errors:")
            for error in errors[:5]:  # Show first 5 errors
                logger.warning(f"  - {error}")

        self.stats['transformed'] = len(df)

        # Prepare for warehouse
        warehouse_data = transformer.prepare_for_warehouse(df)

        logger.info(f"Transformation complete: {len(df)} records transformed")
        logger.info(f"Warehouse tables prepared: {list(warehouse_data.keys())}")

        return warehouse_data

    def _load(self, warehouse_data):
        """Load data into warehouse"""

        logger.info("\n" + "=" * 80)
        logger.info("STEP 3: LOAD")
        logger.info("=" * 80)

        with WarehouseLoader(str(self.db_path)) as loader:
            results = loader.load_all(warehouse_data)

        self.stats['loaded'] = sum(results.values())

        logger.info(f"Load complete: {results}")

    def _compute_similarities(self):
        """Compute anime similarity scores"""

        logger.info("\n" + "=" * 80)
        logger.info("STEP 4: COMPUTE SIMILARITIES")
        logger.info("=" * 80)

        engine = SimilarityEngine(
            db_path=str(self.db_path),
            max_features=1000,
            top_n=50
        )

        engine.compute_and_store()

        # Count similarities
        import duckdb
        conn = duckdb.connect(str(self.db_path))
        count = conn.execute("SELECT COUNT(*) FROM fact_anime_similarity").fetchone()[0]
        conn.close()

        self.stats['similarities_computed'] = count

        logger.info(f"Similarity computation complete: {count} scores stored")

    def _run_quality_checks(self):
        """Run data quality checks"""

        logger.info("\n" + "=" * 80)
        logger.info("STEP 5: QUALITY CHECKS")
        logger.info("=" * 80)

        checker = DataQualityChecker(str(self.db_path))
        checker.add_standard_checks()

        results = checker.run_all()

        self.stats['quality_score'] = results['quality_score']

        # Print summary
        logger.info(f"Quality checks complete:")
        logger.info(f"  - Total checks: {results['total_checks']}")
        logger.info(f"  - Passed: {results['passed']}")
        logger.info(f"  - Failed: {results['failed']}")
        logger.info(f"  - Quality score: {results['quality_score']:.1f}%")

        # Log critical failures
        critical_failures = [
            c for c in results['checks']
            if c['severity'] == 'critical' and c['status'] == 'failed'
        ]

        if critical_failures:
            logger.error(f"Found {len(critical_failures)} CRITICAL quality failures:")
            for check in critical_failures:
                logger.error(f"  - {check['name']}: {check['message']}")

    def _print_summary(self):
        """Print pipeline execution summary"""

        duration = (self.stats['end_time'] - self.stats['start_time']).total_seconds()

        print("\n" + "=" * 80)
        print("ETL PIPELINE SUMMARY")
        print("=" * 80)
        print(f"Duration: {duration:.2f} seconds")
        print(f"Extracted: {self.stats['extracted']} records")
        print(f"Transformed: {self.stats['transformed']} records")
        print(f"Loaded: {self.stats['loaded']} records")

        if not self.skip_similarity:
            print(f"Similarities computed: {self.stats['similarities_computed']}")

        if not self.skip_quality:
            print(f"Quality score: {self.stats['quality_score']:.1f}%")

        print("=" * 80)


# ==============================================================================
# MAIN EXECUTION
# ==============================================================================

def main():
    """Main execution function"""

    parser = argparse.ArgumentParser(
        description='Run the anime ETL pipeline'
    )

    parser.add_argument(
        '--limit',
        type=int,
        default=100,
        help='Number of anime to extract (default: 100)'
    )

    parser.add_argument(
        '--incremental',
        action='store_true',
        help='Extract only updates from last 24 hours'
    )

    parser.add_argument(
        '--skip-similarity',
        action='store_true',
        help='Skip similarity computation (faster)'
    )

    parser.add_argument(
        '--skip-quality',
        action='store_true',
        help='Skip quality checks'
    )

    parser.add_argument(
        '--db-path',
        type=str,
        default=str(WAREHOUSE_DB_PATH),
        help=f'Path to warehouse database (default: {WAREHOUSE_DB_PATH})'
    )

    args = parser.parse_args()

    # Run pipeline
    pipeline = ETLPipeline(
        db_path=args.db_path,
        limit=args.limit,
        incremental=args.incremental,
        skip_similarity=args.skip_similarity,
        skip_quality=args.skip_quality
    )

    success = pipeline.run()

    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()

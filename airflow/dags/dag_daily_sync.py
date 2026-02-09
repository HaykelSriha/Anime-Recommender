"""
DAG: Daily Anime Sync
Schedule: 2 AM UTC daily
Pipeline: Extract (AniList) -> Transform -> Load -> Similarity -> Quality Checks
"""

import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

# Project root is volume-mounted at /opt/airflow/project
PROJECT_ROOT = Path('/opt/airflow/project')
sys.path.insert(0, str(PROJECT_ROOT))

WAREHOUSE_PATH = str(PROJECT_ROOT / 'warehouse' / 'anime_full_phase1.duckdb')

default_args = {
    'owner': 'anime-recommender',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}


def run_extract(**context):
    """Extract anime data from AniList API."""
    from etl.extract.anilist_extractor import AniListExtractor

    extractor = AniListExtractor()
    raw_data = extractor.extract_top_anime(limit=1000, sort='POPULARITY_DESC')

    # Store to disk for next task (avoids XCom size limits for large datasets)
    import json
    staging_path = str(PROJECT_ROOT / 'data' / 'staging' / 'daily_extract.json')
    Path(staging_path).parent.mkdir(parents=True, exist_ok=True)
    with open(staging_path, 'w', encoding='utf-8') as f:
        json.dump(raw_data, f, ensure_ascii=False, default=str)

    context['ti'].xcom_push(key='extracted_count', value=len(raw_data))
    context['ti'].xcom_push(key='staging_path', value=staging_path)
    print(f"Extracted {len(raw_data)} anime from AniList")
    return len(raw_data)


def run_transform(**context):
    """Transform extracted data for warehouse loading."""
    import json
    from etl.transform.anime_transformer import AnimeTransformer

    staging_path = context['ti'].xcom_pull(key='staging_path', task_ids='extract')
    with open(staging_path, 'r', encoding='utf-8') as f:
        raw_data = json.load(f)

    transformer = AnimeTransformer(
        remove_html=True,
        normalize_genres=True,
        calculate_metrics=True,
    )
    df, errors = transformer.transform(raw_data)
    warehouse_data = transformer.prepare_for_warehouse(df)

    # Store warehouse-ready data to staging
    import pickle
    warehouse_staging = str(PROJECT_ROOT / 'data' / 'staging' / 'daily_warehouse.pkl')
    with open(warehouse_staging, 'wb') as f:
        pickle.dump(warehouse_data, f)

    context['ti'].xcom_push(key='warehouse_staging', value=warehouse_staging)
    context['ti'].xcom_push(key='transform_errors', value=len(errors))
    print(f"Transformed {len(df)} anime ({len(errors)} errors)")
    return len(df)


def run_load(**context):
    """Load transformed data into DuckDB warehouse."""
    import pickle
    from etl.load.warehouse_loader import WarehouseLoader

    warehouse_staging = context['ti'].xcom_pull(key='warehouse_staging', task_ids='transform')
    with open(warehouse_staging, 'rb') as f:
        warehouse_data = pickle.load(f)

    with WarehouseLoader(WAREHOUSE_PATH) as loader:
        results = loader.load_all(warehouse_data)

    print(f"Loaded into warehouse: {results}")
    return results


def run_similarity(**context):
    """Recompute TF-IDF similarity scores."""
    from etl.ml.similarity_engine import SimilarityEngine

    engine = SimilarityEngine(db_path=WAREHOUSE_PATH, max_features=1000, top_n=50)
    engine.compute_and_store()
    print("Similarity matrix recomputed")


def run_quality_checks(**context):
    """Run data quality checks. Fail task on critical issues."""
    from etl.quality.quality_checks import DataQualityChecker

    checker = DataQualityChecker(WAREHOUSE_PATH)
    checker.add_standard_checks()
    results = checker.run_all()
    checker.print_report(results)

    context['ti'].xcom_push(key='quality_score', value=results['quality_score'])

    # Fail the task if quality is below threshold
    critical_failures = [
        c for c in results['checks']
        if c['severity'] == 'critical' and c['status'] == 'failed'
    ]
    if critical_failures:
        raise ValueError(
            f"Quality score {results['quality_score']:.1f}% - "
            f"Critical failures: {[c['name'] for c in critical_failures]}"
        )

    print(f"Quality score: {results['quality_score']:.1f}%")
    return results['quality_score']


with DAG(
    dag_id='anime_daily_sync',
    default_args=default_args,
    description='Daily AniList extraction, transformation, loading, and quality checks',
    schedule_interval='0 2 * * *',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['etl', 'daily'],
) as dag:

    extract = PythonOperator(
        task_id='extract',
        python_callable=run_extract,
    )

    transform = PythonOperator(
        task_id='transform',
        python_callable=run_transform,
    )

    load = PythonOperator(
        task_id='load',
        python_callable=run_load,
    )

    similarity = PythonOperator(
        task_id='compute_similarity',
        python_callable=run_similarity,
    )

    quality = PythonOperator(
        task_id='quality_checks',
        python_callable=run_quality_checks,
    )

    extract >> transform >> load >> similarity >> quality

"""
DAG: Weekly Full Refresh
Schedule: Sunday 1 AM UTC
Pipeline: Full extraction -> Populate metrics -> Retrain ML models -> Health check
"""

import os
import sys
import importlib.util
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
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
    'execution_timeout': timedelta(hours=2),
}


def _load_script(script_name):
    """Dynamically load a script from the scripts/ directory."""
    script_path = str(PROJECT_ROOT / 'scripts' / script_name)
    spec = importlib.util.spec_from_file_location(
        script_name.replace('.py', ''),
        script_path,
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def run_phase1_extraction(**context):
    """Full Phase 1: extraction from AniList + deduplication."""
    os.chdir(str(PROJECT_ROOT))
    from etl.phase1_orchestrator import Phase1Orchestrator

    orchestrator = Phase1Orchestrator(warehouse_path=WAREHOUSE_PATH)
    orchestrator.run(limit=1000)
    print("Phase 1 extraction complete")


def run_populate_metrics(**context):
    """Fetch fresh metrics (scores, popularity) from AniList API."""
    os.chdir(str(PROJECT_ROOT))
    mod = _load_script('populate_metrics.py')
    mod.main()
    print("Metrics populated from AniList API")


def run_retrain_models(**context):
    """Retrain TF-IDF + NMF models on all anime."""
    os.chdir(str(PROJECT_ROOT))
    mod = _load_script('retrain_models.py')
    mod.main()
    print("ML models retrained (TF-IDF + NMF)")


def run_health_check(**context):
    """Post-refresh health check. Fail if unhealthy."""
    os.chdir(str(PROJECT_ROOT))
    mod = _load_script('health_check.py')
    healthy = mod.health_check()

    context['ti'].xcom_push(key='health_status', value='HEALTHY' if healthy else 'DEGRADED')

    if not healthy:
        raise ValueError("Health check failed after weekly refresh")

    print("Health check passed")
    return healthy


def push_to_github(**context):
    """Commit and push updated database to GitHub so Streamlit Cloud redeploys."""
    import subprocess

    os.chdir(str(PROJECT_ROOT))

    # Configure git for the commit
    subprocess.run(['git', 'config', 'user.email', 'airflow@anime-recommender'], check=True)
    subprocess.run(['git', 'config', 'user.name', 'Airflow Pipeline'], check=True)

    # Stage the updated database
    subprocess.run(['git', 'add', 'warehouse/anime_full_phase1.duckdb'], check=True)

    # Check if there are changes to commit
    result = subprocess.run(['git', 'diff', '--cached', '--quiet'], capture_output=True)
    if result.returncode == 0:
        print("No database changes to push")
        return

    # Commit and push
    subprocess.run(
        ['git', 'commit', '-m', 'Automated data refresh by Airflow pipeline'],
        check=True,
    )
    subprocess.run(['git', 'push'], check=True)
    print("Updated database pushed to GitHub -> Streamlit Cloud will redeploy")


with DAG(
    dag_id='anime_weekly_full_refresh',
    default_args=default_args,
    description='Weekly full re-extraction and ML model retraining',
    schedule_interval='0 1 * * 0',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['etl', 'weekly', 'ml'],
) as dag:

    phase1 = PythonOperator(
        task_id='phase1_extraction',
        python_callable=run_phase1_extraction,
    )

    metrics = PythonOperator(
        task_id='populate_metrics',
        python_callable=run_populate_metrics,
    )

    retrain = PythonOperator(
        task_id='retrain_models',
        python_callable=run_retrain_models,
    )

    health = PythonOperator(
        task_id='health_check',
        python_callable=run_health_check,
    )

    push = PythonOperator(
        task_id='push_to_github',
        python_callable=push_to_github,
    )

    phase1 >> metrics >> retrain >> health >> push

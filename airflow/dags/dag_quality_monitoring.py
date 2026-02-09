"""
DAG: Data Quality Monitoring
Schedule: Every hour
Pipeline: [Health check, Quality checks] -> Alert on failure
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
    'retry_delay': timedelta(minutes=2),
}


def run_health_check(**context):
    """Run warehouse health check."""
    os.chdir(str(PROJECT_ROOT))

    script_path = str(PROJECT_ROOT / 'scripts' / 'health_check.py')
    spec = importlib.util.spec_from_file_location('health_check', script_path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)

    healthy = mod.health_check()
    status = 'HEALTHY' if healthy else 'DEGRADED'
    context['ti'].xcom_push(key='health_status', value=status)
    print(f"Health status: {status}")
    return healthy


def run_quality_checks(**context):
    """Run full quality check suite."""
    from etl.quality.quality_checks import DataQualityChecker

    checker = DataQualityChecker(WAREHOUSE_PATH)
    checker.add_standard_checks()
    results = checker.run_all()
    checker.print_report(results)

    context['ti'].xcom_push(key='quality_score', value=results['quality_score'])
    print(f"Quality score: {results['quality_score']:.1f}%")
    return results['quality_score']


def alert_on_failure(**context):
    """Check upstream results and raise alerts if thresholds breached."""
    health = context['ti'].xcom_pull(key='health_status', task_ids='health_check')
    score = context['ti'].xcom_pull(key='quality_score', task_ids='quality_checks')

    alerts = []

    if health and health != 'HEALTHY':
        alerts.append(f"Health check status: {health}")

    if score is not None and score < 95.0:
        alerts.append(f"Quality score below threshold: {score:.1f}%")

    if alerts:
        alert_msg = "MONITORING ALERTS:\n" + "\n".join(f"  - {a}" for a in alerts)
        print(alert_msg)
        raise ValueError(alert_msg)

    print("All monitoring checks passed. No alerts.")


with DAG(
    dag_id='anime_quality_monitoring',
    default_args=default_args,
    description='Hourly data quality monitoring and alerting',
    schedule_interval='0 * * * *',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['monitoring', 'quality'],
) as dag:

    health = PythonOperator(
        task_id='health_check',
        python_callable=run_health_check,
    )

    quality = PythonOperator(
        task_id='quality_checks',
        python_callable=run_quality_checks,
    )

    alert = PythonOperator(
        task_id='alert_on_failure',
        python_callable=alert_on_failure,
        trigger_rule='all_done',  # Run even if upstream fails
    )

    [health, quality] >> alert

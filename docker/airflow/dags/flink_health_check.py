"""
Flink Health Check DAG

Runs every 5 minutes to verify all Flink jobs are running.
Checks each job's REST API endpoint for RUNNING status.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
import logging

logger = logging.getLogger(__name__)

# Flink jobs to monitor (name, internal URL)
FLINK_JOBS = [
    ('raw-events', 'http://playback-raw-events-jm:8081'),
    ('content-metrics', 'http://playback-content-metrics-jm:8081'),
    ('session-detection', 'http://playback-session-detection-jm:8081'),
    ('lakehouse', 'http://playback-lakehouse-jm:8081'),
]

default_args = {
    'owner': 'playback-platform',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='flink_health_check',
    default_args=default_args,
    description='Monitor Flink job health status',
    schedule_interval='*/5 * * * *',  # Every 5 minutes
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['monitoring', 'flink', 'health-check'],
) as dag:

    def check_flink_job(job_name: str, job_url: str):
        """Check if a Flink job is running"""
        try:
            # Get job overview
            response = requests.get(f"{job_url}/jobs/overview", timeout=10)
            response.raise_for_status()
            jobs = response.json().get('jobs', [])

            # Check if any job is in RUNNING state
            running_jobs = [j for j in jobs if j.get('state') == 'RUNNING']

            if running_jobs:
                logger.info(f"Flink job '{job_name}' is RUNNING")
                return True
            else:
                logger.warning(f"Flink job '{job_name}' is NOT running. States: {[j.get('state') for j in jobs]}")
                return False
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to connect to Flink job '{job_name}': {e}")
            return False

    def check_all_jobs(**context):
        """Check all Flink jobs and report status"""
        results = {}
        all_healthy = True

        for job_name, job_url in FLINK_JOBS:
            is_running = check_flink_job(job_name, job_url)
            results[job_name] = is_running
            if not is_running:
                all_healthy = False

        # Log summary
        logger.info(f"Flink Health Check Results: {results}")

        if not all_healthy:
            failed_jobs = [name for name, healthy in results.items() if not healthy]
            error_msg = f"ALERT: Flink jobs not running: {failed_jobs}"
            logger.error(error_msg)
            # In production: send to Slack, PagerDuty, etc.
            raise Exception(error_msg)

        logger.info("All Flink jobs are healthy")
        return results

    check_jobs = PythonOperator(
        task_id='check_all_flink_jobs',
        python_callable=check_all_jobs,
    )

    def log_metrics(**context):
        """Log metrics for Prometheus (via pushgateway in production)"""
        ti = context['task_instance']
        results = ti.xcom_pull(task_ids='check_all_flink_jobs')
        if results:
            healthy_count = sum(1 for v in results.values() if v)
            logger.info(f"Flink health metrics: {healthy_count}/{len(results)} jobs healthy")

    log_health_metrics = PythonOperator(
        task_id='log_health_metrics',
        python_callable=log_metrics,
    )

    check_jobs >> log_health_metrics

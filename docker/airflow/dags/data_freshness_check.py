"""
Data Freshness Check DAG

Runs every 15 minutes to verify data is flowing through the pipeline.
Alerts if the latest event in Iceberg is older than threshold.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
import logging

logger = logging.getLogger(__name__)

# Maximum allowed data lag (in minutes)
MAX_DATA_LAG_MINUTES = 30

default_args = {
    'owner': 'playback-platform',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='data_freshness_check',
    default_args=default_args,
    description='Check if data is flowing through the pipeline',
    schedule_interval='*/15 * * * *',  # Every 15 minutes
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['monitoring', 'data-quality', 'iceberg'],
) as dag:

    # Query Trino to get the latest event timestamp
    check_latest_event = BashOperator(
        task_id='check_latest_event',
        bash_command='''
            docker exec playback-trino trino --execute "
                SELECT MAX(event_timestamp) as latest_event
                FROM iceberg.warehouse.playback_events
            " 2>/dev/null || echo "1970-01-01 00:00:00"
        ''',
        do_xcom_push=True,
    )

    def evaluate_freshness(**context):
        """Check if data is fresh enough"""
        ti = context['task_instance']
        result = ti.xcom_pull(task_ids='check_latest_event')

        if not result or '1970-01-01' in str(result):
            logger.warning("No data found or query failed")
            return 'alert_stale_data'

        try:
            # Parse the timestamp from Trino output
            latest_str = result.strip().split('\n')[-1].strip('"')
            latest = datetime.fromisoformat(latest_str.replace(' ', 'T'))
            now = datetime.utcnow()
            lag_minutes = (now - latest).total_seconds() / 60

            logger.info(f"Latest event: {latest}, Lag: {lag_minutes:.1f} minutes")

            if lag_minutes > MAX_DATA_LAG_MINUTES:
                logger.warning(f"Data lag ({lag_minutes:.1f} min) exceeds threshold ({MAX_DATA_LAG_MINUTES} min)")
                return 'alert_stale_data'
            else:
                logger.info(f"Data is fresh (lag: {lag_minutes:.1f} min)")
                return 'data_is_fresh'
        except Exception as e:
            logger.error(f"Error parsing timestamp: {e}")
            return 'alert_stale_data'

    evaluate = BranchPythonOperator(
        task_id='evaluate_freshness',
        python_callable=evaluate_freshness,
    )

    def log_fresh():
        logger.info("Data freshness check passed")

    data_is_fresh = PythonOperator(
        task_id='data_is_fresh',
        python_callable=log_fresh,
    )

    def send_alert(**context):
        """Send alert about stale data (stub for now)"""
        logger.error("ALERT: Data is stale! Check Flink jobs and Kafka.")
        # In production: send to Slack, PagerDuty, etc.

    alert_stale_data = PythonOperator(
        task_id='alert_stale_data',
        python_callable=send_alert,
    )

    check_latest_event >> evaluate >> [data_is_fresh, alert_stale_data]

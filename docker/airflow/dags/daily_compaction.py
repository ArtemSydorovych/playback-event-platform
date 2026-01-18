"""
Daily Compaction DAG

Runs daily at 3 AM to compact Iceberg tables:
- Compact small files
- Rewrite manifests
- Expire old snapshots
- Remove orphan files
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import logging

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'playback-platform',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
}

with DAG(
    dag_id='daily_compaction',
    default_args=default_args,
    description='Daily Iceberg table maintenance using Spark',
    schedule_interval='0 3 * * *',  # Run at 3 AM daily
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['spark', 'iceberg', 'maintenance'],
) as dag:

    # Run the compaction Spark job
    run_compaction = BashOperator(
        task_id='run_compaction',
        bash_command='''
            docker exec playback-spark-master /opt/spark/bin/spark-submit \
                --conf spark.sql.shuffle.partitions=4 \
                /opt/spark-jobs/iceberg_compaction.py
        ''',
        execution_timeout=timedelta(hours=2),
    )

    def log_table_stats(**context):
        """Log table statistics after compaction"""
        logger.info("Compaction completed. Use Trino or Spark to verify table health.")
        # In production, you'd query table metadata here

    log_stats = PythonOperator(
        task_id='log_table_stats',
        python_callable=log_table_stats,
    )

    run_compaction >> log_stats

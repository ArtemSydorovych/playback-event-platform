"""
Hourly Aggregation DAG

Runs hourly to aggregate playback events from Iceberg into hourly stats table.
Triggers Spark job: hourly_aggregation.py
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.http.sensors.http import HttpSensor
import logging

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'playback-platform',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='hourly_aggregation',
    default_args=default_args,
    description='Aggregate playback events hourly using Spark',
    schedule_interval='5 * * * *',  # Run at 5 minutes past each hour
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['spark', 'aggregation', 'iceberg'],
) as dag:

    # Check if Spark master is available
    check_spark_master = HttpSensor(
        task_id='check_spark_master',
        http_conn_id='spark_master',
        endpoint='/',
        request_params={},
        response_check=lambda response: response.status_code == 200,
        poke_interval=30,
        timeout=300,
    )

    # Run the hourly aggregation Spark job
    run_hourly_aggregation = BashOperator(
        task_id='run_hourly_aggregation',
        bash_command='''
            docker exec playback-spark-master /opt/spark/bin/spark-submit \
                --conf spark.sql.shuffle.partitions=4 \
                /opt/spark-jobs/hourly_aggregation.py
        ''',
    )

    def log_completion(**context):
        execution_date = context['execution_date']
        logger.info(f"Hourly aggregation completed for {execution_date}")

    log_success = PythonOperator(
        task_id='log_success',
        python_callable=log_completion,
    )

    check_spark_master >> run_hourly_aggregation >> log_success

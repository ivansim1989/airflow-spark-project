from airflow import DAG
from pyspark.sql import SparkSession
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta
from scripts.setup_logger import setup_logging

logger = setup_logging()

# Define callback functions
def on_success_task(dict):
    """Log a success message when a task succeeds."""
    logger.info('Task succeeded: %s', dict)

def on_failure_task(dict):
    """Log an error message when a task fails."""
    logger.error('Task failed: %s', dict)

default_args = {
    'owner': 'Airflow',
    'retries':3,
    'retry_delay': timedelta(seconds=60),
    'emails': ['ivansim0831@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'on_failure_callback': on_failure_task,
    'on_success_callback': on_success_task,
    'execution_time': timedelta(seconds=60)
}



with DAG("process_csv", start_date=datetime(2023, 5, 1), schedule_interval=timedelta(days=1), default_args=default_args, catchup=False) as dag:

    process_csv = SparkSubmitOperator(
        task_id="process_csv",
        application="/opt/airflow/dags/scripts/process_csv.py",
        conn_id="spark_conn",
        application_args=["applications_dataset_1.csv", "applications_dataset_2.csv"],
        verbose= False
    )

    process_csv
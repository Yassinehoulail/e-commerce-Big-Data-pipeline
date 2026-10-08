"""
E-commerce Pipeline DAG
Orchestrates the Big Data Medallion Architecture pipeline:
scraper → sync_to_minio → bronze_to_silver → silver_to_gold → load_to_mysql
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import logging

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email': ['admin@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Create the DAG
dag = DAG(
    'ecommerce_pipeline_dag',
    default_args=default_args,
    description='E-commerce Big Data Medallion Architecture Pipeline',
    schedule_interval='@daily',  # Run every day at midnight
    catchup=False,  # Don't run past DAG runs
    tags=['ecommerce', 'bigdata', 'medallion'],
)

# Task 1: Run the scraper
scraper_task = BashOperator(
    task_id='run_scraper',
    bash_command='cd /opt/airflow/scraper && python scraper.py',
    dag=dag,
)

# Task 2: Sync CSV files to MinIO
sync_to_minio_task = BashOperator(
    task_id='sync_to_minio',
    bash_command='cd /opt/airflow/minio_scripts && python sync_to_minio.py',
    dag=dag,
)

# Task 3: Transform bronze to silver
bronze_to_silver_task = BashOperator(
    task_id='bronze_to_silver',
    bash_command='cd /opt/airflow/transform && python bronze_to_silver.py',
    dag=dag,
)

# Task 4: Transform silver to gold
silver_to_gold_task = BashOperator(
    task_id='silver_to_gold',
    bash_command='cd /opt/airflow/transform && python silver_to_gold.py',
    dag=dag,
)

# Task 5: Load gold data to MySQL
load_to_mysql_task = BashOperator(
    task_id='load_to_mysql',
    bash_command='cd /opt/airflow && python load_to_mysql.py',
    dag=dag,
)

# Define task dependencies
# scraper → sync_to_minio → bronze_to_silver → silver_to_gold → load_to_mysql
scraper_task >> sync_to_minio_task >> bronze_to_silver_task >> silver_to_gold_task >> load_to_mysql_task

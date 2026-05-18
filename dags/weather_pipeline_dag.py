from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess

default_args = {
    'owner': 'kiplangat',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

def run_producer():
    subprocess.Popen(
        ['python3', '/root/kafka/weather_docker/producer_weather_data.py'],
    )
    print("Producer started!")

def run_consumer():
    subprocess.Popen(
        ['python3', '/root/kafka/weather_docker/consumer_weather_data.py'],
    )
    print("Consumer started!")

with DAG(
    dag_id='weather_streaming_pipeline',
    default_args=default_args,
    description='Real-time weather streaming pipeline',
    schedule='*/10 * * * *',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['weather', 'kafka', 'confluent'],
) as dag:

    start_producer = PythonOperator(
        task_id='start_producer',
        python_callable=run_producer,
    )

    start_consumer = PythonOperator(
        task_id='start_consumer',
        python_callable=run_consumer,
    )

    start_producer >> start_consumer

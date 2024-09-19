from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime




start_date = datetime.today() - timedelta(days=1)

default_args = {
    "owner": "airflow",
    "start_date": start_date,
    "retries": 1,
    "retry_delay": timedelta(seconds=5),
}

with DAG(
    dag_id="hydrometrie_kafka_spark_dag",
    default_args=default_args,
    schedule=timedelta(hours=1),  # Run every hour
    catchup=False,
) as dag:

    # Task to send data to Kafka
    kafka_stream_task = PythonOperator(
        task_id="kafka_hydrometrie_data_stream",
        python_callable=stream_hydrometrie_data,
        dag=dag,
    )

    # Spark task to consume and process Kafka data
    spark_stream_task = DockerOperator(
        task_id="pyspark_consumer_hydrometrie",
        image="hydrometrie_spark:latest",
        api_version="auto",
        auto_remove=True,
        command="./bin/spark-submit --master local[*] --packages org.postgresql:postgresql:42.5.4,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 ./spark_streaming.py",
        docker_url='tcp://docker-proxy:2375',
        environment={'SPARK_LOCAL_HOSTNAME': 'localhost'},
        network_mode="airflow-kafka",
        dag=dag,
    )

    kafka_stream_task >> spark_stream_task
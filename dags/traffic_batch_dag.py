from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

with DAG(
    dag_id="daily_traffic_batch_report",
    start_date=datetime(2025, 1, 1),
    schedule_interval="0 4 * * *",  
    catchup=False,
    tags=["spark", "traffic"],
) as dag:
    SPARK_CONN_ID = "spark_default"
    find_peak_traffic_task = SparkSubmitOperator(
        task_id="find_daily_peak_traffic",
        application="/opt/spark/app/batch_report.py", 
        conn_id=SPARK_CONN_ID,
        packages="org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1", 
        spark_binary="/opt/spark/bin/spark-submit",
        conf={
            "spark.driver.host": "airflow-scheduler", 
            "spark.driver.bindAddress": "0.0.0.0",
            "spark.jars.ivy": "/tmp/.ivy2"
        },
        dag=dag,
    )
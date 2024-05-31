import airflow 
from datetime import timedelta
from datetime import datetime
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from functions.func_xcom_push import run_spark_and_push_to_xcom
from functions.func_xcom_pull import load_to_postgresql
import os
from dotenv import load_dotenv

load_dotenv()

connect_to_postgres = os.environ.get('postgres_connect_db')

spark_args = { 
'owner': 'airflow', 
'start_date': datetime(2024,5,17),
'depends_on_past': False, 
'email': ['airflow@example.com'], 
'email_on_failure': False, 
'email_on_retry': False}


dag_spark = DAG( 
dag_id = "sparkoperator_data", 
default_args = spark_args, 
schedule_interval='0 * * * *', 
dagrun_timeout=timedelta(minutes=60), 
description='use case of sparkoperator in airflow', 
start_date = airflow.utils.dates.days_ago(1))


spark_submit_local = SparkSubmitOperator( 
application ='/airflow/scripts/spark_script.py', 
conn_id= 'spark_default', 
task_id='spark_submit_task', 
dag=dag_spark)


sql_create_database = PostgresOperator(
    task_id = 'sql_command',
    postgres_conn_id = connect_to_postgres,
	sql = """CREATE TABLE IF NOT EXISTS tiktok_spark (
       reviewId VARCHAR NOT NULL,
       userName VARCHAR NOT NULL,
       userImage VARCHAR NOT NULL,
       content VARCHAR,
       score SMALLINT NOT NULL,
	   thumbsUpCount SMALLINT NOT NULL,
	   reviewCreatedVersion VARCHAR NOT NULL,
	   replyContent VARCHAR NOT NULL,
	   repliedAt VARCHAR NOT NULL,
	   date_time TIMESTAMP);""",
	   dag=dag_spark
	   )


save_path_to_xcom = PythonOperator(
    task_id='save_path_to_xcom',
    python_callable = run_spark_and_push_to_xcom,
    provide_context=True,
    dag=dag_spark)


load_data_sql = PythonOperator(
    task_id='load_data_to_postgres',
    python_callable=load_to_postgresql,
    provide_context=True,
    dag=dag_spark,
)
   
spark_submit_local >> save_path_to_xcom >> sql_create_database  >> load_data_sql


if __name__ == "__main__": 
	dag_spark.cli()
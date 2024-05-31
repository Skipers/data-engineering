import airflow 
from datetime import timedelta
from datetime import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago
from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
import os


spark_args = { 
'owner': 'airflow', 
'start_date': datetime(2024,5,17),
'depends_on_past': False, 
'email': ['airflow@example.com'], 
'email_on_failure': False, 
'email_on_retry': False}


dag_spark = DAG( 
dag_id = "sparkoperator_demo_sql_load", 
default_args = spark_args, 
schedule_interval='0 * * * *', 
dagrun_timeout=timedelta(minutes=60), 
description='use case of sparkoperator in airflow', 
start_date = airflow.utils.dates.days_ago(1))


def run_spark_and_push_to_xcom(**kwargs):
    result_path = 'file:/home/vboxuser/Documents/GitHub/data-engineering/task.csv'
    print(f"Сохранение пути к файлу в XCom: {result_path}")
    if os.path.exists(result_path):
        kwargs['ti'].xcom_push(key='result_path', value=result_path)
    else:
        raise FileNotFoundError(f"Файл не найден по пути: {result_path}")


spark_submit_local = SparkSubmitOperator( 
application ='/airflow/scripts/func2.py', 
conn_id= 'spark_default', 
task_id='spark_submit_task', 
dag=dag_spark)


sql_create_database = PostgresOperator(
   task_id = 'sql_command',
   postgres_conn_id = 'main_postgres_connection',
	sql = """CREATE TABLE IF NOT EXISTS tiktok_spark (
       reviewId VARCHAR NOT NULL,
       userName VARCHAR NOT NULL,
       userImage VARCHAR NOT NULL,
       content VARCHAR NOT NULL,
       score SMALLINT NOT NULL,
	   thumbsUpCount SMALLINT NOT NULL,
	   reviewCreatedVersion VARCHAR NOT NULL,
	   replyContent VARCHAR NOT NULL,
	   repliedAt VARCHAR NOT NULL,
	   date_time TIMESTAMP);""",
	   dag=dag_spark
	   )


def load_to_postgresql(**kwargs):
    ti = kwargs['ti']
    result_path = ti.xcom_pull(task_ids='spark_submit_task', key='result_path')
    print(f"Путь к файлу, извлеченный из XCom: {result_path}")
    pg_hook = PostgresHook(postgres_conn_id='main_postgres_connection')
    
    with open(result_path, 'r') as file:
        pg_hook.copy_expert(sql="COPY tiktok_spark FROM STDIN WITH CSV HEADER", filename=result_path)
        
        
load_data_sql = PythonOperator(
    task_id='load_data_to_postgres',
    python_callable=load_to_postgresql,
    provide_context=True,
    dag=dag_spark,
)
   
spark_submit_local >> sql_create_database >> load_data_sql


if __name__ == "__main__": 
	dag_spark.cli()
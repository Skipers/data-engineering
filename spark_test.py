from airflow import DAG
from datetime import datetime
from airflow.operators.bash_operator import BashOperator
from airflow.hooks.base_hook import BaseHook
# from airflow.providers.postgres.operators.postgres import PostgresOperator

connection = BaseHook.get_connection("main_postgres_connection")

default_args = {
    'owner': 'task5',
    'depends_on_past': False,
    'start_date': datetime(2024,4,10)
}


dag = DAG(
    'task5',
    default_args=default_args,
    schedule_interval='0 * * * *',
    catchup=True,
    max_active_runs=1
)




spark_task = BashOperator(
    task_id = 'task5',
    bash_command='python3 /airflow/scripts/func1.py --date {{ ds }} ' +f'--host {connection.host} --dbname {connection.schema} --user {connection.login} --jdbc_password {connection.password} --port 5432',
    dag=dag

)

# spark_task = PostgresOperator(
# task_id = 'task5',
# postgres_conn_id= 'main_postgres_connection',
# )
spark_task












# spark_config = {
#     'conf': {
#         'spark.yarn.maxAppAttempts': '1',
#         'spark.yarn.executor.memoryOverhead': '512'
# },
# 'conn_id': 'spark_local',
# 'application': 'python3 /airflow/scripts/func.py',
# 'driver_menory': '1g',
# 'executor_cores': 1,
# 'num_executors': 1,
# 'executor_menory': '1g'
# }

# submit_job = SparkSubmitOperator(
#     task_id='submit_job',
#     dag=dag, **spark_config)


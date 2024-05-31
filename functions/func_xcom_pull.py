from airflow.hooks.postgres_hook import PostgresHook


def load_to_postgresql(**kwargs):
    ti = kwargs['ti']
    result_path = ti.xcom_pull(task_ids='save_path_to_xcom', key='result_path')
    print(f"Путь к файлу, извлеченный из XCom: {result_path}")
    pg_hook = PostgresHook(postgres_conn_id='main_postgres_connection')
    
    with open(result_path, 'r') as file:
        pg_hook.copy_expert(sql="COPY tiktok_spark FROM STDIN WITH CSV HEADER", filename=result_path)
        
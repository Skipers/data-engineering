import os


def run_spark_and_push_to_xcom(**kwargs):
    result_path = '/home/vboxuser/Documents/GitHub/data-engineering/task.csv'
    print(f"Сохранение пути к файлу в XCom: {result_path}")
    if os.path.exists(result_path):
        kwargs['ti'].xcom_push(key='result_path', value=result_path)
    else:
        raise FileNotFoundError(f"Файл не найден по пути: {result_path}")
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, to_timestamp
import pandas as pd
import os

def extract_data(file_type, file_path):
    spark = SparkSession.builder \
        .appName("TASKA") \
        .getOrCreate()

    df = spark.read.format(file_type).option('header', 'True') \
        .load(file_path)
    return df

extract_df = extract_data('csv', '/home/vboxuser/Documents/GitHub/data-engineering/files/tiktok.csv')


def transform_data(df):
    df = df.withColumn('content', regexp_replace('content', "[^A-Za-zА-Яа-я\s.,!?]", ""))
    df = df.withColumn('date_time', to_timestamp(df['at'], 'yyyy-MM-dd HH:mm:ss'))
    df = df.drop('at')
    df = df.na.fill('-')
    df = df.na.drop()
    # df = df.dropna(subset = ['date_time'])
    return df

transformed_df = transform_data(extract_df)
transformed_df.show(vertical=True)


def load_data(df, file_path_save):
    if os.path.isdir(file_path_save):
        raise ValueError(f"Указанный путь является директорией: {file_path_save}")
    
    task = df.toPandas()
    task.to_csv(file_path_save, index=False)
    return task

file_path_save = '/home/vboxuser/Documents/GitHub/data-engineering/task.csv'
save_df = load_data(transformed_df, file_path_save)
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, to_timestamp


# def extract_data():
spark = SparkSession.builder \
    .appName("TASKA") \
    .getOrCreate()

df = spark.read.format('csv').option('header', 'True') \
    .load('python3 /airflow/scripts/tiktok_google_play_reviews')
    
df.show()
#     return df

# def transform_data(df):
#     df = df.na.fill('-')
#     df = df.withColumn('content', regexp_replace('content', "[^A-Za-zА-Яа-я\s.,!?]", ""))
#     df = df.withColumn('date_time', to_timestamp(df['at'], 'yyyy-MM-dd HH:mm:ss'))
#     df = df.drop('at')
#     return df



# def load_data(df):
#     df.write.format("com.mongodb.spark.sql.DefaultSource") \
#         .mode("overwrite") \
#         .option("uri", "mongodb://localhost:27017/tasks") \
#         .load()

# if __name__ == "__main__":
#     df = extract_data()
#     df_transformed = transform_data(df)
#     load_data(df_transformed)

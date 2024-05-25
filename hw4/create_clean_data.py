from pyspark.sql.types import *
from pyspark.sql import SparkSession
import transformers


spark = SparkSession.builder \
    .appName("create-table") \
    .enableHiveSupport() \
    .getOrCreate()


YC_SOURCE_BUCKET = 'bucket-mlops-fraud-system' 
df = spark.read.option('comment', '#').\
                option('timestampFormat', 'yyyy-MM-dd HH:mm:ss').\
                schema("tranaction_id LONG, tx_datetime STRING, customer_id INT, terminal_id INT, tx_amount DOUBLE, tx_time_seconds LONG, tx_time_days LONG, tx_fraud INT, tx_fraud_scenario INT").\
                format('csv').\
                load(f's3a://{YC_SOURCE_BUCKET}/data/*.txt')

data_filter = transformers.DataFilter()
transformed_df = data_filter.transform(df)

transformed_df.write.parquet(f"s3a://{YC_SOURCE_BUCKET}/cleaned_data/data_clean.parquet", mode="overwrite")
import findspark
findspark.init()

import os

from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

import transformers
import mlflow
import logging

from pathlib import Path
import yaml

# load config file
config_path = Path(__file__).parent / "config.yaml"
with open(config_path, "r") as file:
    config = yaml.load(file, Loader=yaml.FullLoader)


logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()



class ModelInference():
    def __init__(self, model_name = config["MODEL_NAME"]):
        spark = SparkSession.builder \
                        .appName("model_inference") \
                        .getOrCreate()
        
        os.environ["AWS_ACCESS_KEY_ID"] = config["S3_KEY_ID"]
        os.environ["AWS_SECRET_ACCESS_KEY"] = config["S3_SECRET_KEY"]
        os.environ["MLFLOW_S3_ENDPOINT_URL"] = "https://storage.yandexcloud.net"
        
        self.model = self.load_model(model_name)
        self.spark = spark
        self.data_filter = transformers.DataFilter()
        
    def load_model(self, model_name):
        self.model_name = model_name
        loaded_model = mlflow.spark.load_model(f's3://{config["SOURCE_BUCKET"]}/artifacts/1/{model_name}/artifacts/fraud_detection_model.mlmodel')
        return loaded_model
   
    def get_forecast(self, msg):
        df = self.spark.createDataFrame(data = [msg])
        transformed_df = self.data_filter.transform(df)
        predictions_test = self.model.transform(transformed_df)
        
        predictions_test = predictions_test.withColumn('tx_datetime',col('tx_datetime').cast('string'))
        predictions_test = predictions_test.select(['tranaction_id', 'tx_datetime', 'prediction', 'probability'])
        predictions_test = predictions_test.withColumn('probability',col('probability').cast('string'))
        
        predictions_test = predictions_test.toPandas()
        #self.save_forecast(predictions_test, msg[5])
        return predictions_test.to_dict()


    
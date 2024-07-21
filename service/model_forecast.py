import findspark
findspark.init()


from typing import Optional, List
import os

from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, lit, col
from pydantic import BaseModel
import transformers
import mlflow
import pickle
import json
from datetime import datetime
import logging

from fastapi import FastAPI, HTTPException
#from starlette_exporter import PrometheusMiddleware, handle_metrics

SOURCE_BUCKET = 'bucket-mlops-fraud-system' 
S3_KEY_ID = 'YCAJERcdEYXXGtibDA_bKmuCN'
S3_SECRET_KEY = 'YCOhTcO5kxCoBY950-36WcWo6uzy8tBJ4S1gxEsP'

app = FastAPI()


logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()

class ModelHandler:
    def __init__(self):
        self.model = None

class Transaction(BaseModel):
    tranaction_id: str
    tx_datetime: str
    customer_id: str
    terminal_id: str
    tx_amount: str
    tx_time_seconds: str
    tx_time_days: str
    tx_fraud: Optional[str] = None
    tx_fraud_scenario: Optional[str] = None


MODEL_NAME = '99aebbd9217e4802a3d911c58e0b4111'
MODEL = ModelHandler()

class ModelInference():
    def __init__(self, model_name):
        spark = SparkSession.builder \
                        .appName("model_inference") \
                        .getOrCreate()
        
        os.environ["AWS_ACCESS_KEY_ID"] = S3_KEY_ID
        os.environ["AWS_SECRET_ACCESS_KEY"] = S3_SECRET_KEY
        os.environ["MLFLOW_S3_ENDPOINT_URL"] = "https://storage.yandexcloud.net"
        
        self.model = self.load_model(model_name)
        
        self.spark = spark
        self.data_filter = transformers.DataFilter()
        
    def load_model(self, model_name):
        self.model_name = model_name
        loaded_model = mlflow.spark.load_model(f's3://{SOURCE_BUCKET}/artifacts/1/{model_name}/artifacts/fraud_detection_model.mlmodel')
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
    
@app.on_event("startup")
def load_model():
    MODEL.model = ModelInference(MODEL_NAME)

@app.get("/")
def read_healthcheck():
    return {"status": "healthcheck", "version": "0"}

@app.post("/predict")
def predict(msg:Transaction):
    if MODEL.model is None:
        raise HTTPException(status_code=503, detail="No model loaded")
    try:
        result = MODEL.model.get_forecast(msg)
        return result
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


if __name__ == '__main__':
    load_model()
    
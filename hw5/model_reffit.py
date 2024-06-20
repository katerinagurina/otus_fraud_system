import findspark
findspark.init()

import os
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler, MinMaxScaler
from pyspark.ml.classification import GBTClassifier
from pyspark.mllib.evaluation import MulticlassMetrics
from pyspark.ml import Pipeline

import mlflow
from mlflow.tracking import MlflowClient

from pyspark.sql.functions import col, when, rand

SELECTED_FEATURES = [#'tranaction_id',
                                         #'tx_datetime',
                                         'customer_id',
                                         'terminal_id',
                                         'tx_amount',
                                         #'tx_time_seconds',
                                         #'tx_time_days',
                                         #'tx_fraud',
                                         #'tx_fraud_scenario',
                                         'day_of_week',
                                         'week_of_year',
                                         'is_weekend',
                                         'hour',
                                         'is_day',
                                         'avg_amount_per_customer_for_1_days',
                                         'number_of_tx_per_customer_for_1_days',
                                         'avg_amount_per_customer_for_7_days',
                                         'number_of_tx_per_customer_for_7_days',
                                         'avg_amount_per_customer_for_30_days',
                                         'number_of_tx_per_customer_for_30_days',
                                         #'number_of_tx_per_terminal_for_1_day',
                                         #'number_of_fraud_tx_per_terminal_for_1_day',
                                         #'number_of_tx_per_terminal_for_7_day',
                                         #'number_of_fraud_tx_per_terminal_for_7_day',
                                         #'number_of_tx_per_terminal_for_30_day',
                                         #'number_of_fraud_tx_per_terminal_for_30_day'
]

FILES_FOR_TRAINING = ['2019-08-22','2019-09-21', '2019-10-21']
FILES_FOR_TESTING = ['2019-11-20']

SOURCE_BUCKET = 'bucket-mlops-fraud-system/cleaned_data/' 
S3_KEY_ID = ''
S3_SECRET_KEY = ''
TRACKING_SERVER_HOST = '178.154.205.20'

def get_pipeline():
    numericAssembler = VectorAssembler()\
                        .setInputCols(SELECTED_FEATURES)\
                        .setOutputCol("features")
    
    scaler = MinMaxScaler()\
            .setInputCol("features")\
            .setOutputCol("scaledFeatures")
    
    classifier = GBTClassifier(featuresCol='scaledFeatures',
                    labelCol='tx_fraud',
                    maxDepth = 3,
                    weightCol = "classWeights",
                    subsamplingRate = 0.8
                    )
    pipeline = Pipeline(stages=[numericAssembler, scaler, classifier])
    return pipeline

def calculate_metric_values(predictions):
    predictions = predictions.withColumn('tx_fraud', predictions['tx_fraud'].cast('double'))
    predictions = predictions.withColumn('prediction', predictions['prediction'].cast('double'))

    results = predictions.select(['prediction', 'tx_fraud'])
    predictionAndLabels=results.rdd
    metrics = MulticlassMetrics(predictionAndLabels)

    cm=metrics.confusionMatrix().toArray()

    precision=(cm[0][0])/(cm[0][0]+cm[1][0])
    recall=(cm[0][0])/(cm[0][0]+cm[0][1])
    return precision, recall

def make_unioned_df(file_names):
    for i, fname in enumerate(file_names):
        data = spark.read.load(f's3a://{SOURCE_BUCKET}/clean_{fname}.parquet')
        data = data.withColumn("classWeights", when(data.tx_fraud==1, 5).otherwise(1))
        if i == 0:
            unioned_df = data
        else:
            unioned_df = unioned_df.union(data) 

    return unioned_df



spark = SparkSession.builder \
    .appName("model_reffit") \
    .getOrCreate()


os.environ["MLFLOW_S3_ENDPOINT_URL"] = "https://storage.yandexcloud.net"
os.environ["AWS_ACCESS_KEY_ID"] = S3_KEY_ID
os.environ["AWS_SECRET_ACCESS_KEY"] = S3_SECRET_KEY

mlflow.set_tracking_uri(f"http://{TRACKING_SERVER_HOST}:8000")
mlflow.set_experiment("pyspark_experiment_3")
    
inf_pipeline = get_pipeline()

train_unioned_df = make_unioned_df(FILES_FOR_TRAINING)
model = inf_pipeline.fit(train_unioned_df)

predictions_train = model.transform(train_unioned_df)
precision_train, recall_train = calculate_metric_values(predictions_train)
# print(precision_train, recall_train)

mlflow.log_metric("Precision on train", precision)
mlflow.log_metric("Recall on train", recall)

test_unioned_df = make_unioned_df(FILES_FOR_TESTING)
predictions_test = model.transform(test_unioned_df)
precision_test, recall_test = calculate_metric_values(predictions_test)
# print(precision_test, recall_test)


mlflow.log_metric("Precision on test", precision_test)
mlflow.log_metric("Recall on test", recall_test)

mlflow.spark.save_model(model, "model_for_fraud_detection.mlmodel")



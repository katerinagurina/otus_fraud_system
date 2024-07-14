import findspark
findspark.init()
from pyspark.sql.types import *
from pyspark.sql import SparkSession

import os

import mlflow
from mlflow.tracking import MlflowClient
from pyspark.sql.functions import col, when, rand
import pyspark.sql.functions as F

from scipy.stats import norm, beta, ttest_ind, ks_2samp
from scipy.stats import t
import pandas as pd
from datetime import datetime
import io
import boto3
import logging
import numpy as np
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import DoubleType, IntegerType, StringType

from pyspark.ml.linalg import Vectors, VectorUDT

S3_KEY_ID = 'YCAJERcdEYXXGtibDA_bKmuCN'
S3_SECRET_KEY = 'YCOhTcO5kxCoBY950-36WcWo6uzy8tBJ4S1gxEsP'
TRACKING_SERVER_HOST = '158.160.116.60'
FILES_FOR_AB_TESTING = ['2019-12-20']
SOURCE_BUCKET = 'bucket-mlops-fraud-system' 
MODEL_NAME = '99aebbd9217e4802a3d911c58e0b4111'

today = datetime.today().strftime('%Y_%m_%d')
FILE_NAME = f'forecast_for_the_{today}.csv'


logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()

SPARK = SparkSession.builder \
        .appName("ab_test") \
        .getOrCreate()

SPARK._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "storage.yandexcloud.net")
SPARK._jsc.hadoopConfiguration().set("fs.s3a.signing-algorithm", "")
SPARK._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
SPARK._jsc.hadoopConfiguration().set("fs.s3a.access.key",S3_KEY_ID)
SPARK._jsc.hadoopConfiguration().set("fs.s3a.secret.key", S3_SECRET_KEY)

def ttest_ind_pyspark(data1, data2, alpha = 0.05):
    mean1, mean2 = data1.agg(F.mean('probability')).collect()[0][0], data2.agg(F.mean('probability')).collect()[0][0]
    std1, std2 = data1.agg(F.stddev('probability')).collect()[0][0], data2.agg(F.stddev('probability')).collect()[0][0]
    n1, n2 = data1.count(), data2.count()
    se1, se2 = std1/np.sqrt(n1), std2/np.sqrt(n2)
    sed = np.sqrt(se1**2.0 + se2**2.0)
    t_stat = (mean1 - mean2) / (sed +1e-5)
    df = n1 + n2 - 2
    cv = t.ppf(1.0 - alpha, df)
    p = (1.0 - t.cdf(abs(t_stat), df)) * 2.0
    return p

def load_model(run_id, experiment_id_reffit):
    loaded_model = mlflow.spark.load_model(f's3://{SOURCE_BUCKET}/artifacts/{experiment_id_reffit}/{run_id}/artifacts/fraud_detection_model.mlmodel')
    return loaded_model

def get_probability_distribution_by_model(test_df, model_run_id,experiment_id_reffit):
    loaded_model = load_model(model_run_id, experiment_id_reffit)
    predictions = loaded_model.transform(test_df)
    firstelement=F.udf(lambda v:float(v[0]),FloatType())
    secondelement = F.udf(lambda v:float(v[1]),FloatType())
    predictions = predictions.withColumn('probability', when(col('prediction')==1, firstelement('probability')).otherwise(secondelement('probability')))
    return predictions

def make_unioned_df(file_names):
    for i, fname in enumerate(file_names):
        data = SPARK.read.load(f's3a://{SOURCE_BUCKET}/cleaned_data/clean_{fname}.parquet')
        if i == 0:
            unioned_df = data
        else:
            unioned_df = unioned_df.union(data) 
    return unioned_df

def main():
    os.environ["MLFLOW_S3_ENDPOINT_URL"] = "https://storage.yandexcloud.net"
    os.environ["AWS_ACCESS_KEY_ID"] = S3_KEY_ID
    os.environ["AWS_SECRET_ACCESS_KEY"] = S3_SECRET_KEY

    mlflow.set_tracking_uri(f"http://{TRACKING_SERVER_HOST}:8000")
   
    # Prepare MLFlow experiment for logging
    client = MlflowClient()
    experiment_name = 'AB testing of probabilities'
    experiment = client.get_experiment_by_name(experiment_name)

    if experiment is None:
        mlflow.set_experiment(experiment_name)
        experiment = client.get_experiment_by_name(experiment_name)

    experiment_id = experiment.experiment_id
    # Добавьте в название вашего run имя, по которому его можно будет найти в MLFlow
    run_name = 'AB test time: ' + ' ' + str(datetime.now())

    
    experiment_name_reffit = 'pyspark_experiment_for_model_reffit'
    experiment_reffit = client.get_experiment_by_name(experiment_name_reffit)
    if experiment_reffit is None:
        logger.info('There is no trained model in MLFlow')
        experiment_id_reffit = 1
    else:
        experiment_id_reffit = experiment_reffit.experiment_id

    with mlflow.start_run(run_name=run_name, experiment_id=experiment_id):
        logger.info('Loading testing df and get probabilities')
        test_df = make_unioned_df(FILES_FOR_AB_TESTING)
        model_probabilities = get_probability_distribution_by_model(test_df, MODEL_NAME, experiment_id_reffit)

        logger.info('Loading data from s3 for the last day')
        schema = StructType([
                    StructField("tx_id", LongType()),
                    StructField("datetime", StringType()),
                    StructField("prediction", DoubleType()),
                    StructField("probability", StringType()),
                ])
        probabilities_for_the_last_day = SPARK.read.csv(f's3a://{SOURCE_BUCKET}/model_forecast/{FILE_NAME}', schema = schema, header = True)
        arraytovector = F.udf(lambda vs: Vectors.dense(vs), VectorUDT())
        probabilities_for_the_last_day = probabilities_for_the_last_day.withColumn('probability', arraytovector(F.from_json('probability', "array<double>")))
        
        firstelement=F.udf(lambda v:float(v[0]),FloatType())
        secondelement = F.udf(lambda v:float(v[1]),FloatType())
        probabilities_for_the_last_day = probabilities_for_the_last_day.withColumn('probability', when(col('prediction')==1, firstelement('probability')).otherwise(secondelement('probability')))

        logger.info('Making ab test')
        p_value = ttest_ind_pyspark(model_probabilities, probabilities_for_the_last_day)
        alpha = 0.05
        if p_value < alpha:
            ab_test_result = "Вероятности из разных распредений"
            flag_for_model_reffit = True
        else:
            ab_test_result = "Вероятности из одного распределения"
            flag_for_model_reffit = False


        mlflow.log_metric("P_value", p_value)
        mlflow.log_param("Result", ab_test_result)
        mlflow.log_param("Flag for model reffit", flag_for_model_reffit)
        
        SPARK.stop()


if __name__ == "__main__":
    main()
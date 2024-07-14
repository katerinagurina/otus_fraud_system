import findspark
findspark.init()

import os

from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler, MinMaxScaler
from pyspark.ml.classification import GBTClassifier
from pyspark.mllib.evaluation import MulticlassMetrics
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml import Pipeline

from datetime import datetime
import numpy as np
import mlflow
from mlflow.tracking import MlflowClient

from pyspark.ml.tuning import CrossValidator, ParamGridBuilder

from pyspark.sql.functions import col, when, rand
import logging

SELECTED_FEATURES = ['customer_id',
                     'terminal_id',
                     'tx_amount',
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
                     'number_of_tx_per_customer_for_30_days']

FILES_FOR_TRAINING = ['2019-08-22', '2019-09-21','2019-10-21', '2019-11-20', '2019-12-20']

#Для различий в моделях
FILES_FOR_TRAINING = list(np.random.choice(FILES_FOR_TRAINING, size = 3))
FILES_FOR_TESTING = ['2020-01-19']

SOURCE_BUCKET = 'bucket-mlops-fraud-system/' 
S3_KEY_ID = 'YCAJERcdEYXXGtibDA_bKmuCN'
S3_SECRET_KEY = 'YCOhTcO5kxCoBY950-36WcWo6uzy8tBJ4S1gxEsP'
TRACKING_SERVER_HOST = '178.154.225.3'

SPARK = SparkSession.builder \
    .appName("model_reffit") \
    .getOrCreate()

SPARK._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "storage.yandexcloud.net")
SPARK._jsc.hadoopConfiguration().set("fs.s3a.signing-algorithm", "")
SPARK._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
SPARK._jsc.hadoopConfiguration().set("fs.s3a.access.key",S3_KEY_ID)
SPARK._jsc.hadoopConfiguration().set("fs.s3a.secret.key", S3_SECRET_KEY)

logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()

# def get_pipeline():

#     #pipeline = Pipeline(stages=[numericAssembler, scaler])
#     return pipeline

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
        data = SPARK.read.load(f's3a://{SOURCE_BUCKET}/cleaned_data/clean_{fname}.parquet')
        data = data.withColumn("classWeights", when(data.tx_fraud==1, 5).otherwise(1))
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
    #mlflow.set_experiment("pyspark_experiment_4")
    # Prepare MLFlow experiment for logging
    client = MlflowClient()


    # Create an experiment if it doesn't exist
    experiment_name = 'pyspark_experiment_for_model_reffit'
    experiment = client.get_experiment_by_name(experiment_name)

    if experiment is None:
        mlflow.set_experiment(experiment_name)
        experiment = client.get_experiment_by_name(experiment_name)

    experiment_id = experiment.experiment_id

    # Добавьте в название вашего run имя, по которому его можно будет найти в MLFlow
    run_name = 'Run time: ' + ' ' + str(datetime.now())

    with mlflow.start_run(run_name=run_name, experiment_id=experiment_id):
        numericAssembler = VectorAssembler()\
                        .setInputCols(SELECTED_FEATURES)\
                        .setOutputCol("features")
    
        scaler = MinMaxScaler()\
            .setInputCol("features")\
            .setOutputCol("scaledFeatures")
    
        classifier = GBTClassifier(featuresCol='scaledFeatures',
                    labelCol='tx_fraud',
                    weightCol = "classWeights",
                    )
        pipeline = Pipeline(stages=[numericAssembler, scaler, classifier])
        
        logger.info('Loading data')
        train_unioned_df = make_unioned_df(FILES_FOR_TRAINING)
        #train_unioned_df = train_unioned_df.sample(0.05)
        paramGrid = ParamGridBuilder().addGrid(classifier.maxDepth, [3, 5, 10])\
                                        .addGrid(classifier.maxIter, [5, 10, 20])\
                                        .addGrid(classifier.subsamplingRate, [0.7, 0.8, 0.9])\
                                        .build()
        #paramGrid = ParamGridBuilder().addGrid(classifier.maxDepth, [2, 3]).build()
        gbtEval = BinaryClassificationEvaluator(labelCol='tx_fraud')
        
        # 5-fold cross validation with grid search
        crossval = CrossValidator(estimator=pipeline, estimatorParamMaps=paramGrid, evaluator=gbtEval, numFolds=2)
        logger.info('Running cross-validation')
        cv_model = crossval.fit(train_unioned_df)
        auc_train = cv_model.avgMetrics[0]
       


        predictions_train = cv_model.transform(train_unioned_df)


        logger.info('Calculating metrics on train')
        precision_train, recall_train  = calculate_metric_values(predictions_train)

        mlflow.log_metric("Precision on train", precision_train)
        mlflow.log_metric("Recall on train", recall_train)
        mlflow.log_metric("ROC AUC on cv", auc_train)

        logger.info('Making prediction on test')
        test_unioned_df = make_unioned_df(FILES_FOR_TESTING)
        
        logger.info('Calculating metric on test')
        predictions_test = cv_model.transform(test_unioned_df)

        auc_test = gbtEval.evaluate(predictions_test, {gbtEval.metricName: "areaUnderROC"})
        precision_test, recall_test = calculate_metric_values(predictions_test)

        mlflow.log_metric("Precision on test", precision_test)
        mlflow.log_metric("Recall on test", recall_test)
        mlflow.log_metric("ROC AUC on test", auc_test)

        
        logger.info('Fitting model with the best params')
        best_model = cv_model.bestModel

        logger.info('Saving model')
        mlflow.spark.log_model(best_model, 'fraud_detection_model.mlmodel')
    SPARK.stop()



if __name__ == "__main__":
    main()
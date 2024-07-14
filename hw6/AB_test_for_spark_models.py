import findspark
findspark.init()
from pyspark.sql.types import *
from pyspark.sql import SparkSession

import os

import mlflow
from mlflow.tracking import MlflowClient
from pyspark.sql.functions import col, when, rand
import pyspark.sql.functions as F

from scipy.stats import norm, beta, ttest_ind
from scipy.stats import t

from datetime import datetime
import numpy as np
import logging

S3_KEY_ID = 'YCAJERcdEYXXGtibDA_bKmuCN'
S3_SECRET_KEY = 'YCOhTcO5kxCoBY950-36WcWo6uzy8tBJ4S1gxEsP'
TRACKING_SERVER_HOST = '158.160.116.60'
FILES_FOR_AB_TESTING = ['2019-12-20']
SOURCE_BUCKET = 'bucket-mlops-fraud-system' 
# MODELS_TO_COMPARE = {'first_model': '62ec5ce8ad354d6f937e196af8b1fd5a',
#                     'second_model' : '06ded67866054c0a8aafbfda870f05d2'}
MODELS_TO_COMPARE = None


logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()

SPARK = SparkSession.builder \
        .appName("ab_test") \
        .getOrCreate()

def get_N(metric_for_1_model, metric_for_2_model, confidence_level = 0.95, power = 0.8):
    effect_size = metric_for_1_model - metric_for_2_model
    z_alpha = norm.ppf(1 - (1 - confidence_level) / 2)
    z_beta = abs(norm.ppf(1 - power))

    sample_size = ((2 * (z_alpha + z_beta)**2 * (metric_for_1_model * (1 - metric_for_1_model))) / effect_size**2)

    # Округляем до целого значения
    sample_size = int(sample_size) + 1
    return sample_size

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

def get_probability_distribution(test_df, model_run_id,experiment_id_reffit):
    loaded_model = load_model(model_run_id, experiment_id_reffit)
    predictions = loaded_model.transform(test_df)
	#predictions = predictions.withColumn('rand', rand(seed=23))
	#predictions = predictions.withColumn('prediction', (predictions.rand>=0.5).cast('int'))
	#predictions = predictions.withColumn('result', (predictions.tx_fraud == predictions.prediction).cast('int'))
	#res_df = predictions.select('result', 'prediction').toPandas()
    #res_df = predictions.select('prediction').toPandas()
    #return res_df['prediction'].values
    firstelement=F.udf(lambda v:float(v[0]),FloatType())
    secondelement = F.udf(lambda v:float(v[1]),FloatType())
    predictions = predictions.withColumn('probability', when(col('prediction')==1, firstelement('probability')).otherwise(secondelement('probability')))
    return predictions

def make_unioned_df(file_names):
    for i, fname in enumerate(file_names):
        data = SPARK.read.load(f's3a://{SOURCE_BUCKET}/cleaned_data/clean_{fname}.parquet')
        #data = data.withColumn("classWeights", when(data.tx_fraud==1, 5).otherwise(1))
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
    experiment_name = 'AB testing expirement'
    experiment = client.get_experiment_by_name(experiment_name)

    if experiment is None:
        mlflow.set_experiment(experiment_name)
        experiment = client.get_experiment_by_name(experiment_name)

    experiment_id = experiment.experiment_id
    # Добавьте в название вашего run имя, по которому его можно будет найти в MLFlow
    run_name = 'AB test time: ' + ' ' + str(datetime.now())

    experiment_name_reffit = 'pyspark_experiment_for_model_reffit'
    experiment_reffit = client.get_experiment_by_name(experiment_name_reffit)
    experiment_id_reffit = experiment_reffit.experiment_id
    if experiment_reffit is None:
        logger.info('There is no trained model in MLFlow')
    else:
        all_runs = mlflow.search_runs(experiment_names=[experiment_name_reffit])
        #Если не задано имя лучшей модели, то делаем сравнение для двух лучших моделей из mlflow по ROC_AUC на cv
        if MODELS_TO_COMPARE is None:
            all_runs = all_runs[all_runs['status']=='FINISHED'].sort_values(by = 'metrics.ROC AUC on cv')
            if len(all_runs)>=2:
                model_run_id_new, model_run_id_current = all_runs.iloc[:2]['run_id'].values
                logger.info(f'Models to compare: {model_run_id_new}, {model_run_id_current}')
                roc_auc_model_new, roc_auc_model_current = all_runs.iloc[:2]['metrics.ROC AUC on cv'].values
                flag_to_stop = False
            else:
                logger.info('Please fit models or set names of models')
                flag_to_stop = True
        else:
            model_run_id_new = MODELS_TO_COMPARE['first_model']
            model_run_id_current = MODELS_TO_COMPARE['second_model']
            logger.info(f'Models to compare: {model_run_id_new}, {model_run_id_current}')
            roc_auc_model_new = all_runs[all_runs['run_id']==model_run_id_new]['metrics.ROC AUC on cv'].values
            roc_auc_model_current = all_runs[all_runs['run_id']==model_run_id_current]['metrics.ROC AUC on cv'].values

        N = get_N(roc_auc_model_current, roc_auc_model_new)
        test_df = make_unioned_df(FILES_FOR_AB_TESTING)
        if flag_to_stop==False:
            if test_df.count()>N :
                # test_sample_df_1 = test_df.orderBy(F.rand()).limit(N)
                prob_dist_model_current = get_probability_distribution(test_df, model_run_id_current, experiment_id_reffit)

                # test_sample_df_2 = test_df.orderBy(F.rand()).limit(N)
                prob_dist_model_new = get_probability_distribution(test_df, model_run_id_new, experiment_id_reffit)

                #делаем тест отличиются ли распределения вероятностей двух моделей
                #t_statistic, p_value = ttest_ind(prob_dist_model_new.select('probability').toPandas(), prob_dist_model_current.select('probability').toPandas())

                p_value = ttest_ind_pyspark(prob_dist_model_new, prob_dist_model_current)
                alpha = 0.05
                if p_value < alpha:
                    result = "статистически значимое различие"
                    best_model_name = model_run_id_new
                else:
                    result = "нет статистически значимого различия"
                    best_model_name = model_run_id_current

                with mlflow.start_run(run_name=run_name, experiment_id=experiment_id):
                    mlflow.log_metric("P_value", p_value)
                    mlflow.log_param("First model run id", model_run_id_current)
                    mlflow.log_param("Second model run id", model_run_id_new)
                    mlflow.log_param("Best model run id", best_model_name)

            else:
                logger.info("There is no enough samples for AB test")
                with mlflow.start_run(run_name=run_name, experiment_id=experiment_id):
                    mlflow.log_param("Best model run id", None)
            
        SPARK.stop()


if __name__ == "__main__":
    main()
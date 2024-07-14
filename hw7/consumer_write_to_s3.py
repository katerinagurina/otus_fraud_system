#!/usr/bin/env python
"""OTUS BigData ML kafka consumer example"""
import json
import argparse

import kafka 

# import utils_for_inference
import logging
from tqdm import tqdm
from typing import Dict, NamedTuple
import boto3
import pandas as pd
import io
from datetime import datetime

USERNAME = 'gurina'
PASSWORD = 'Since1995'
DEFAULT_SERVERS = ['rc1a-14lc5lsh7v75c27c.mdb.yandexcloud.net:9091',
                   'rc1a-uhav0330q3julo0c.mdb.yandexcloud.net:9091',
                   'rc1b-9fcij75bqlffr7eg.mdb.yandexcloud.net:9091',
                   'rc1b-ltvai1u2f6m4dduk.mdb.yandexcloud.net:9091',
                   'rc1d-ki7fd2gse9qv21sl.mdb.yandexcloud.net:9091',
                   'rc1d-nrlecsol49iik0nn.mdb.yandexcloud.net:9091']

GROUP_ID = 'second'

INPUT_TOPIC = 'output_data'
SOURCE_BUCKET = 'bucket-mlops-fraud-system' 

today = datetime.today().strftime('%Y_%m_%d')
FILE_NAME = f'forecast_for_the_{today}.csv'

S3_KEY_ID = 'YCAJERcdEYXXGtibDA_bKmuCN'
S3_SECRET_KEY = 'YCOhTcO5kxCoBY950-36WcWo6uzy8tBJ4S1gxEsP'


logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()

def serialize(msg: Dict) -> bytes:
    return json.dumps(msg).encode("utf-8")

def main():
    

    session = boto3.session.Session()
    s3_client = session.client(
        service_name='s3',
        endpoint_url='https://storage.yandexcloud.net',
        aws_access_key_id=S3_KEY_ID,
        aws_secret_access_key=S3_SECRET_KEY
    )

    consumer = kafka.KafkaConsumer(
    bootstrap_servers=DEFAULT_SERVERS,
    security_protocol="SASL_SSL",
    sasl_mechanism="SCRAM-SHA-512",
    sasl_plain_username=USERNAME,
    sasl_plain_password=PASSWORD,
    #ssl_cafile="/usr/local/share/ca-certificates/Yandex/YandexInternalRootCA.crt",
    ssl_cafile="YandexCA.crt",
    group_id=GROUP_ID,
    value_deserializer=json.loads)

    consumer.subscribe(topics=[INPUT_TOPIC])

    

    for msg in tqdm(consumer):
        logger.info(msg)
        df = pd.DataFrame(msg[6])

        #bytes_to_write = df.to_csv(None, header=None, index=False).encode()
        try:
            csv_obj = s3_client.get_object(Bucket=f'{SOURCE_BUCKET}', Key='model_forecast/' + FILE_NAME)
            current_data = csv_obj['Body'].read().decode('utf-8')
            current_df = pd.read_csv(io.StringIO(current_data))

        except:
            current_df = pd.DataFrame()
        # csv_obj = s3_client.get_object(Bucket=f'{SOURCE_BUCKET}', Key=file_name)
        # current_data = csv_obj['Body'].read().decode('utf-8')
                # append
        appended_data = pd.concat([df, current_df], ignore_index=True)
        appended_data_encoded = appended_data.to_csv(None, index=False).encode('utf-8')
        # overwrite
        s3_client.put_object(Body=appended_data_encoded, Bucket=f'{SOURCE_BUCKET}', Key='model_forecast/' + FILE_NAME)

        logger.info('MSG saved')
    

if __name__ == "__main__":
    main()

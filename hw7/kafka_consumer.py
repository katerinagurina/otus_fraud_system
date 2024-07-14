#!/usr/bin/env python
"""OTUS BigData ML kafka consumer example"""
import findspark
findspark.init()

import json
import argparse

import kafka 

# import utils_for_inference
import model_forecast
from typing import Dict, NamedTuple
import logging
from tqdm import tqdm

USERNAME = 'gurina'
PASSWORD = 'Since1995'
DEFAULT_SERVERS = ['rc1a-14lc5lsh7v75c27c.mdb.yandexcloud.net:9091',
                   'rc1a-uhav0330q3julo0c.mdb.yandexcloud.net:9091',
                   'rc1b-9fcij75bqlffr7eg.mdb.yandexcloud.net:9091',
                   'rc1b-ltvai1u2f6m4dduk.mdb.yandexcloud.net:9091',
                   'rc1d-ki7fd2gse9qv21sl.mdb.yandexcloud.net:9091',
                   'rc1d-nrlecsol49iik0nn.mdb.yandexcloud.net:9091']

GROUP_ID = 'first'

INPUT_TOPIC = 'input_data'
OUTPUT_TOPIC = 'output_data'

MODEL_NAME = '99aebbd9217e4802a3d911c58e0b4111'
SOURCE_BUCKET = 'bucket-mlops-fraud-system' 


logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()

def serialize(msg: Dict) -> bytes:
    return json.dumps(msg).encode("utf-8")

def main():
    
    consumer = kafka.KafkaConsumer(
    # 'test_topic',
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

    producer_output = kafka.KafkaProducer(
        bootstrap_servers=DEFAULT_SERVERS,
        security_protocol="SASL_SSL",
        sasl_mechanism="SCRAM-SHA-512",
        sasl_plain_username=USERNAME,
        sasl_plain_password=PASSWORD,
        #ssl_cafile="/home/gurina/local/share/ca-certificates/Yandex/YandexInternalRootCA.crt",
        ssl_cafile="YandexCA.crt",
        value_serializer=serialize)

    model = model_forecast.ModelInference(MODEL_NAME)
    for msg in tqdm(consumer):
        msg_output = model.get_forecast(msg)
        logger.warning(f'MSG OUTPUT:{msg_output}')
        
        future = producer_output.send(topic=OUTPUT_TOPIC,
                                    key=f'{msg[5]}'.encode("ascii"),
                                    value=msg_output)
        logger.info('MSG sent')

        record_metadata = future.get(timeout=1)
    
        #producer_output.flush()
        #producer_output.close()

if __name__ == "__main__":
    main()

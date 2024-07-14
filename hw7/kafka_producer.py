#!/usr/bin/env python
"""OTUS BigData ML kafka producer example"""

import json

import logging
import random
import datetime
import argparse
from collections import namedtuple
import boto3
from typing import Dict, NamedTuple

import kafka
import datetime


USERNAME = 'gurina'
PASSWORD = 'Since1995'
DEFAULT_SERVERS = ['rc1a-14lc5lsh7v75c27c.mdb.yandexcloud.net:9091',
                   'rc1a-uhav0330q3julo0c.mdb.yandexcloud.net:9091',
                   'rc1b-9fcij75bqlffr7eg.mdb.yandexcloud.net:9091',
                   'rc1b-ltvai1u2f6m4dduk.mdb.yandexcloud.net:9091',
                   'rc1d-ki7fd2gse9qv21sl.mdb.yandexcloud.net:9091',
                   'rc1d-nrlecsol49iik0nn.mdb.yandexcloud.net:9091']
INPUT_TOPIC = 'input_data'
N = 5000


SOURCE_BUCKET = 'bucket-mlops-fraud-system' 
S3_KEY_ID = 'YCAJERcdEYXXGtibDA_bKmuCN'
S3_SECRET_KEY = 'YCOhTcO5kxCoBY950-36WcWo6uzy8tBJ4S1gxEsP'
FILE_FOR_MSG_GENERATION = 'data/2019-09-21.txt'

logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()


class RecordMetadata(NamedTuple):
    topic: str
    partition: int
    offset: int

# def send_message(producer: kafka.KafkaProducer, topic: str) -> RecordMetadata:
#     click = generate_click()
#     future = producer.send(
#         topic=topic,
#         key=str(click["page_id"]).encode("ascii"),
#         value=click,
#     )

#     # Block for 'synchronous' sends
#     record_metadata = future.get(timeout=1)
#     return RecordMetadata(
#         topic=record_metadata.topic,
#         partition=record_metadata.partition,
#         offset=record_metadata.offset,
#     )


# def generate_click() -> Dict:
#     return {
#         "ts": datetime.datetime.now().isoformat(),
#         "user_id": random.randint(0, 5),
#         "page_id": random.randint(0, 5),
#     }

def serialize(msg: Dict) -> bytes:
    return json.dumps(msg).encode("utf-8")




def main():
   
    producer = kafka.KafkaProducer(
        bootstrap_servers=DEFAULT_SERVERS,
        security_protocol="SASL_SSL",
        sasl_mechanism="SCRAM-SHA-512",
        sasl_plain_username=USERNAME,
        sasl_plain_password=PASSWORD,
        #ssl_cafile="/home/gurina/local/share/ca-certificates/Yandex/YandexInternalRootCA.crt",
        ssl_cafile="YandexCA.crt",
        value_serializer=serialize)

    try:
        # record_md = send_message(producer, INPUT_TOPIC)
        s3_client = boto3.client('s3',
                            endpoint_url='https://storage.yandexcloud.net',
                            aws_access_key_id=S3_KEY_ID,
                            aws_secret_access_key=S3_SECRET_KEY)
        obj = s3_client.get_object(Bucket=SOURCE_BUCKET, Key=FILE_FOR_MSG_GENERATION)
        
        i = 0
        list_with_messages = []
        for line in obj['Body'].iter_lines():
            decoded_line = line.decode('utf-8')
            if i == 0:
                columns = str(line)[4:-1].replace('#', '').split(' | ')
            else:
                values = str(decoded_line).split(',')  
                msg = {k:v for k, v in zip(columns, values)}
                #logger.info('MSG generated')
                
            i+=1

            if i==1:
                pass
            #Нужно для тестирования и оценки очереди, в прод варианте можно убрать
            elif i<N:
                future = producer.send(
                                    topic=INPUT_TOPIC,
                                    key=f'tx_{i}'.encode("ascii"),
                                    value=msg)
                logger.info('MSG sent')

                record_metadata = future.get(timeout=1)
            else:
                break
    except kafka.errors.KafkaError as err:
        logging.exception(err)
    
    producer.flush()
    producer.close()


if __name__ == "__main__":
    main()



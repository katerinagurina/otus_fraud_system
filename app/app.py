
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Optional
import sys
sys.path.append('../otus_fraud_system/ml/')
from model_forecast import ModelInference
from starlette_exporter import PrometheusMiddleware, handle_metrics

# import json
# import kafka
# import yaml
# from typing import Dict, NamedTuple
# from pathlib import Path
# # load config file
# config_path = Path(__file__).parent / "kafka_config.yaml"
# with open(config_path, "r") as file:
#     config = yaml.load(file, Loader=yaml.FullLoader)


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


MODEL = ModelHandler()

app = FastAPI()
app.add_middleware(PrometheusMiddleware)
app.add_route("/metrics", handle_metrics)

@app.on_event("startup")
def load_model():
    MODEL.model = ModelInference()
    #initiliaze_kafka()
    
@app.get("/healthcheck")
def read_healthcheck():
    return {"status": "healthcheck"}

@app.post("/predict")
def predict(msg:Transaction):
    if MODEL.model is None:
        raise HTTPException(status_code=503, detail="No model loaded")
    try:
        result = MODEL.model.get_forecast(msg)
        return result
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))






# def serialize(msg: Dict) -> bytes:
#     return json.dumps(msg).encode("utf-8")


# def initiliaze_kafka():
#     consumer = kafka.KafkaConsumer(
#     # 'test_topic',
#     bootstrap_servers=config["DEFAULT_SERVERS"],
#     security_protocol="SASL_SSL",
#     sasl_mechanism="SCRAM-SHA-512",
#     sasl_plain_username=config["USERNAME"],
#     sasl_plain_password=config["PASSWORD"],
#     ssl_cafile="YandexCA.crt",
#     group_id=config["GROUP_ID"],
#     value_deserializer=json.loads)

#     consumer.subscribe(topics=[config["INPUT_TOPIC"]])

#     producer_output = kafka.KafkaProducer(
#         bootstrap_servers=config["DEFAULT_SERVERS"],
#         security_protocol="SASL_SSL",
#         sasl_mechanism="SCRAM-SHA-512",
#         sasl_plain_username=config["USERNAME"],
#         sasl_plain_password=config["PASSWORD"],
#         #ssl_cafile="/home/gurina/local/share/ca-certificates/Yandex/YandexInternalRootCA.crt",
#         ssl_cafile="YandexCA.crt",
#         value_serializer=serialize)

#     for msg in consumer:
#         msg_output = predict(msg)
#         future = producer_output.send(topic=config["OUTPUT_TOPIC"],
#                                     key=f'{msg[5]}'.encode("ascii"),
#                                     value=msg_output)
#         future.get(timeout=1)
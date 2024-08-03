
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Optional
import sys
sys.path.append('../otus_fraud_system/ml/')
from model_forecast import ModelInference
from starlette_exporter import PrometheusMiddleware, handle_metrics



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

@app.post("/healthcheck")
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
from fastapi.testclient import TestClient

import sys
 
# setting path
sys.path.append('../otus_fraud_system/app/')

from app import app,load_model

load_model()
client = TestClient(app)

def test_healthcheck():
    response = client.post("/healthcheck")
    assert response.status_code == 200
    assert response.json()["status"] == "healthcheck"


def test_predict():
    data = '{"tranaction_id": "46988237","tx_datetime": "2019-09-21 09:45:59","customer_id": "1","terminal_id": "178","tx_amount": "83.11","tx_time_seconds": "2627159","tx_time_days": "30", "tx_fraud":"0", "tx_fraud_scenario":"0"}'
    response = client.post("/predict", content = 'Content-Type application:json', data = data)
    assert response.status_code == 200
    assert response.json() == {'tranaction_id': {'0': 46988237},
                                'tx_datetime': {'0': '2019-09-21 09:45:59'},
                                'prediction': {'0': 0.0},
                                'probability': {'0': '[0.7829632771264643,0.2170367228735357]'}}

if __name__ == "__main__":
    test_healthcheck()
    test_predict()

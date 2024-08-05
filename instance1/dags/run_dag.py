import json
import requests
from confluent_kafka import Producer
import datetime
from airflow.decorators import dag
from airflow.operators.python import PythonOperator
import time
def send():
    producer_config = {
        'bootstrap.servers': "172.31.2.88:9092",
    }
    producer = Producer(producer_config)
    for i in range(60):
        time.sleep(0.9)
        try:
            res = requests.get('https://api.binance.com/api/v3/ticker/price?symbols=["BTCUSDT","LTCUSDT","ETHUSDT","NEOUSDT","BNBUSDT","QTUMUSDT","EOSUSDT","SNTUSDT","BNTUSDT","GASUSDT","BCCUSDT"]')
            res.raise_for_status()  # Raise an error for bad status codes
            data = res.json()
            current_timestamp = datetime.datetime.utcnow().isoformat()
            for item in data:
                item['timestamp'] = current_timestamp
                item['price'] = float(item['price'])
                producer.produce('coinPrice', value=json.dumps(item))
            producer.flush()
        except Exception as e:
            print(f"An error occurred: {e}")

@dag(
    start_date=datetime.datetime(2023, 7, 24),  # Ensure this is in the past
    schedule_interval='* * * * *',  # Correct parameter name is schedule_interval
    catchup=False
)
def runDag():
    sendApi = PythonOperator(
        task_id="sendApi",
        python_callable=send,
    )

    sendApi

runDag()
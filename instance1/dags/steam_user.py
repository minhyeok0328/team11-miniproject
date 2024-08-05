from datetime import datetime
from airflow.decorators import dag
from airflow.operators.python import PythonOperator
import requests
import json
from confluent_kafka import Producer
import pendulum

# Constants
KAFKA_TOPIC = 'steamusers'
KAFKA_BROKER = '172.31.2.88:9092'
API_KEY = '1D6B63E8C3375FDCE46BD38620595819'
START_USER_ID = 76561197960434622

def create_kafka_producer():
    return Producer({
        'bootstrap.servers': KAFKA_BROKER,
        'client.id': 'steamApi',
        'acks': '0',
        'batch.size': 32768,  # 32KB
        'linger.ms': 10,
        'compression.type': 'snappy',
        'buffer.memory': 67108864  # 64MB
    })

def send_players_to_kafka():
    all_players = []
    user_id = START_USER_ID
    for _ in range(100):
        url = f'http://api.steampowered.com/ISteamUser/GetPlayerSummaries/v0002/?key={API_KEY}&steamids={user_id}'
        print(f"Requesting URL: {url}")
        res = requests.get(url)
        if res.status_code != 200:
            print(f"Failed to fetch player data for {user_id}: {res.status_code}, Response: {res.text}")
            user_id -= 1
            continue
        data = res.json()
        players = data.get('response', {}).get('players', [])
        if players:
            all_players.extend(players)
            print(f"Fetched player data for {user_id}: {players}")
        else:
            print(f"No player found for {user_id}. Proceeding to next user.")
        user_id -= 1

    producer = create_kafka_producer()
    for player in all_players:        
        value = json.dumps(player)
        try:
            producer.produce(KAFKA_TOPIC, value=value)
            print(f"Sent player data to Kafka: {value}")
        except Exception as e:
            print(f"Failed to send message to Kafka: {str(e)}")
    producer.flush()
local_tz = pendulum.timezone("Asia/Seoul")

@dag(
    start_date=datetime(2024, 7, 26, tzinfo=local_tz),
    schedule='*/1 * * * *',  # Every 5 minutes
    catchup=False
)
def steam_user():
    send_players_to_kafka_task = PythonOperator(
        task_id='send_players_to_kafka_task',
        python_callable=send_players_to_kafka,
        provide_context=True,
    )
    send_players_to_kafka_task

dag_instance = steam_user()